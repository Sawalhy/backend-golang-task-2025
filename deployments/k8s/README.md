# Kubernetes manifests

Bonus item (`README.md:260`), and the last row of the build order in
`docs/IMPLEMENTATION.md` §8 — everything mandatory ships first, because this is
the bonus least connected to what is graded.

Plain Kustomize, no Helm. There is one application, three deployments and two
environments; a templating language would add a layer of indirection to
substitute values that a strategic-merge patch already substitutes.

```
deployments/k8s/
├── kind-cluster.yaml          one-node cluster for local verification
├── base/                      the application, and nothing else
│   ├── app.env                non-secret config → ConfigMap (hashed)
│   ├── migrate-job.yaml       schema, once per deploy
│   ├── api-*.yaml             Deployment, Service, HPA, PodDisruptionBudget
│   ├── worker-deployment.yaml consumers + periodic loops
│   ├── relay-deployment.yaml  outbox → RabbitMQ
│   └── networkpolicy.yaml     default-deny ingress, one hole for the API
└── overlays/
    ├── dev/                   + Postgres, RabbitMQ, Redis, seed data
    └── prod/                  + registry, replica counts, resized pools
```

Every file carries its reasoning inline. This page is the parts that span files.

## Run it

```bash
kind create cluster --name orders --config deployments/k8s/kind-cluster.yaml
```

```bash
docker build -t order-processing:0.1.0 -f docker/Dockerfile .
```

```bash
kind load docker-image order-processing:0.1.0 --name orders
```

```bash
kubectl apply -k deployments/k8s/overlays/dev
```

```bash
kubectl wait --for=condition=available --timeout=300s deployment --all -n order-processing
```

Then reach the API:

```bash
kubectl port-forward -n order-processing svc/api 8080:8080
```

The seeded accounts and the smoke test are the same as under Compose — see
`SOLUTION.md`. RabbitMQ's management UI is
`kubectl port-forward -n order-processing svc/rabbitmq 15672:15672`.

Any cluster works; kind is only what this was verified on. `overlays/dev` assumes
a default StorageClass that supports `ReadWriteOnce`, which kind, k3d, minikube,
Docker Desktop and every managed cloud provide.

To tear it down: `kind delete cluster --name orders`.

## What the base deliberately omits

**No datastores.** A production deployment points at managed Postgres, RabbitMQ
and Redis, so shipping them in the base would mean every real environment
inheriting a database it has to override away. `overlays/dev` adds all three for
a laptop; `overlays/prod` adds none.

**No Secret.** `base` references `app-secrets` and does not create it. Applying
the base alone therefore leaves pods in `CreateContainerConfigError` waiting for
four keys:

| Key | |
|---|---|
| `DATABASE_URL` | `postgres://user:pass@host:5432/orders?sslmode=require` |
| `RABBITMQ_URL` | `amqp://user:pass@host:5672/` |
| `JWT_SECRET` | ≥32 bytes; the API refuses to boot without it when `APP_ENV=production` |
| `REDIS_PASSWORD` | may be empty |

`overlays/dev` generates them in the clear, which is what makes them obviously
not secrets. `overlays/prod` expects them to already exist, from External Secrets
or the Secrets Store CSI driver.

**The base defaults to `APP_ENV=production`.** You opt into development, not out
of it. That is also what disables the Swagger UI, which otherwise publishes a
complete map of the API next to the API.

## The connection budget

The one number on these manifests that will take the system down if it is wrong,
and it is not visible in any single file.

Postgres spawns an OS process per connection and saturates at roughly **8–16 for
a four-core instance**. Past that, added connections contend for the same cores
and throughput *drops* — `SOLUTION.md` has the measurement: the pool, not the
CPU, is the bottleneck under load. Kubernetes then multiplies whatever number you
picked by the replica count.

So an HPA is a claim on the database, made by an object that has no idea the
database exists. The constraint:

```
Σ over deployments of (maxReplicas + maxSurge) × DB_MAX_OPEN_CONNS
      < max_connections − headroom
```

`maxSurge` belongs in the sum. A rolling update runs old and new pods together,
so the peak is `replicas + surge` — sizing to the steady state is how a *deploy*,
rather than load, exhausts the pool. Solved for the dev overlay against
`max_connections = 100`, it comes to 80 worst-case against a budget of 90; the
arithmetic is written out in `base/api-deployment.yaml` and restated for the
managed instance in `overlays/prod/kustomization.yaml`.

Three details fall out of it:

- **The worker's pool (8) is smaller than its concurrency (10).** A worker
  waiting on the payment provider holds no transaction and no connection — rule
  4, never do network I/O inside a transaction. If the pool had to cover every
  in-flight job, one 2.4s provider call would pin a connection for its whole
  duration. The pool sizing is the reserve → pay → commit decision showing up in
  a manifest.
- **The relay ignores `DB_MAX_OPEN_CONNS` entirely.** `cmd/relay/main.go:47`
  hardcodes 4, because it runs one claiming transaction at a time and every
  connection it holds is one the API cannot use.
- **`maxReplicas: 6` is the budget solved for replicas**, not an estimate of
  peak traffic. Raising it without lowering `DB_MAX_OPEN_CONNS` is how an HPA
  takes down a database it is not part of.

**And the arithmetic passing is not the same as the shape being right.** 36
steady-state API connections is already well past the 8–16 where a small Postgres
stops going faster. `max_connections` is the limit that throws errors;
saturation is the limit that costs throughput, and it is much lower. See
"PgBouncer" below.

## Shutdown

`terminationGracePeriodSeconds` **must exceed the longest in-flight job**, or a
rolling deploy SIGKILLs a worker mid-payment: the customer is charged, the order
sits in `CHARGING`, and nothing notices until `RecoverStuckCharges` runs minutes
later. Kubernetes defaults to 30s, which is exactly equal to the worker's own
drain budget (`WORKER_SHUTDOWN_TIMEOUT`) and therefore always a photo finish.

| | grace | covers |
|---|---|---|
| api | 30s | 5s preStop + 20s `HTTP_SHUTDOWN_TIMEOUT` |
| worker | 40s | 30s `WORKER_SHUTDOWN_TIMEOUT` + slack |
| relay | 20s | one batch of publishes and their confirms |

The API also has a **5-second `preStop` sleep**, because endpoint removal and
SIGTERM are concurrent rather than ordered: kube-proxy on every node has to
observe the endpoint going away, and until it does it keeps sending new
connections to a pod that has stopped accepting them. That race is the usual
source of 502s during an otherwise clean rolling deploy. It uses the native
`SleepAction` rather than `exec: sleep 5` — the runtime image is distroless and
has no shell to exec.

The same constraint is why the init containers are `/app/migrate version` rather
than a `until pg_isready` loop: there is no shell in the image to run one.
`migrate version` exits non-zero when Postgres is unreachable *or* when no
migration has been applied, and zero otherwise, which is exactly the readiness
condition — and the kubelet's init-container backoff is the retry loop.

**Pod churn was already survivable before any of this**, which is the part worth
defending. Kill a worker mid-charge: the delivery is unacked and redelivered, and
the idempotency key on the `payments` row means the provider charges once. Kill
the relay mid-publish: the outbox row still has `sent_at IS NULL`. Kill an API
pod holding SSE streams: clients reconnect and read current state. A rolling
deploy is the failure this design was built for; these manifests only avoid
provoking it unnecessarily.

## Why the worker and the relay have no probes

No readiness probe: readiness gates Service endpoints, and nothing routes to
either. Both are outbound-only.

No liveness probe, which is the more interesting omission. The failure these
processes actually have is not "the process died" — the kubelet already restarts
that. It is the **wedge**: an AMQP connection drops, every later publish returns
`channel/connection is not open`, and the process stays alive and perfectly
healthy while delivering nothing. It happened here, 241 consecutive failures
before anyone noticed (`SOLUTION.md`, "A bug that only running it would find").

A liveness probe cannot see that. Any endpoint cheap enough to poll every ten
seconds reports on the process, not the pipeline, and would have stayed green for
all 241. What fixed it was treating a broker connection as a session that ends —
`workers.Supervise` redials with jittered backoff — plus outbox lag reported over
the *database* connection so it keeps arriving while the broker is down. **The
signal to alert on is a rising `oldest_age_seconds`, not a probe.**

The probe worth adding has the opposite shape: not "am I alive" but "has this
consumer acked anything since the queue was last non-empty". That is a metrics
question, and it belongs to the Prometheus bonus rather than this one.

## What this needs before it is production

Stated plainly, because silence reads as unfinished.

**PgBouncer.** The real fix for the connection budget: the app's pools become
client connections to PgBouncer, which holds a small server-side pool against
Postgres, and replica count stops being a claim on the database. It is not
shipped here because it is not only a manifest — in transaction pooling mode a
server connection is handed to a different client between statements, so pgx's
prepared-statement cache breaks with `prepared statement "stmtcache_..." already
exists`. It needs `default_query_exec_mode=exec` on the DSN, and shipping that
untested would be worse than sizing the pool honestly and saying so.

**Queue-depth autoscaling for the worker.** There is no HPA on the worker
because CPU is the wrong signal: it spends its time waiting on the payment
provider, so CPU stays flat while the `payments` queue backs up — the metric
would be quietest exactly when scaling is most needed. The right trigger is queue
depth, via KEDA's RabbitMQ scaler or the Prometheus adapter. Either is a new
cluster-level dependency, and neither is a manifest you can verify without
installing it. Scale by hand meanwhile: competing consumers need no code change,
but each replica is 8 more connections.

**Ingress and TLS.** The API Service is a ClusterIP. What sits in front of it —
an Ingress controller, a gateway, a cloud load balancer — differs per cluster,
and baking one choice into the base makes every environment that wants a
different one patch it away. Use `kubectl port-forward` locally.

**Enforced NetworkPolicy.** The policies are written and they apply cleanly, but
enforcement belongs to the CNI, and kind's default kindnet accepts NetworkPolicy
objects and ignores them. Calico, Cilium and the managed cloud CNIs enforce them.
They are also ingress-only on purpose: a default-deny *egress* policy blocks DNS,
every service name stops resolving, and the failure presents as the database
being down rather than as a policy — doing it properly needs an explicit allow to
kube-dns plus one per external dependency, which is a per-cluster decision.

**A `restricted` Pod Security profile in dev.** `base` sets the namespace to
`restricted` and the application pods satisfy it. `overlays/dev` relaxes it to
`baseline`, because the three stock datastore images start as root and drop
privileges in their own entrypoints. Hardening them is possible and pointless:
they are the part of this that never ships.

**Prometheus, and a ServiceMonitor with it.** Separate bonus, not started. The
outbox lag gauge already exists in the relay's logs and is the first thing that
should become a metric.
