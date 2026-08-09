package services

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"

	"github.com/Sawalhy/backend-golang-task-2025/internal/config"
	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/internal/repository"
)

// AuthService issues and verifies JWTs.
//
// The spec mandates JWT authentication but defines no auth endpoints
// (DESIGN_NOTES.md §7) — there is no login route anywhere in README.md, yet
// every protected route needs a token from somewhere. POST /auth/login is
// therefore an addition, called out in the README rather than slipped in.
type AuthService struct {
	store *repository.Store
	cfg   config.AuthConfig
}

func NewAuthService(store *repository.Store, cfg config.AuthConfig) *AuthService {
	return &AuthService{store: store, cfg: cfg}
}

// Claims is what the token carries. Role travels in the token so admin checks
// cost no database round trip — the trade is that a role change does not take
// effect until the current token expires, which at a 24h TTL is acceptable here
// and would want a deny-list in a system with real privilege escalation risk.
type Claims struct {
	UserID uint64          `json:"uid"`
	Role   models.UserRole `json:"role"`
	jwt.RegisteredClaims
}

type Credentials struct {
	Email    string
	Password string
}

type RegisterInput struct {
	Email    string
	Password string
	Name     string
	Role     models.UserRole
}

func (s *AuthService) Register(ctx context.Context, in RegisterInput) (*models.User, error) {
	email := strings.TrimSpace(strings.ToLower(in.Email))
	if email == "" || !strings.Contains(email, "@") {
		return nil, fmt.Errorf("%w: a valid email is required", models.ErrInvalidInput)
	}
	if len(in.Password) < 8 {
		return nil, fmt.Errorf("%w: password must be at least 8 characters", models.ErrInvalidInput)
	}
	if strings.TrimSpace(in.Name) == "" {
		return nil, fmt.Errorf("%w: name is required", models.ErrInvalidInput)
	}

	role := in.Role
	if role != models.RoleAdmin {
		role = models.RoleCustomer
	}

	// bcrypt, not SHA-256: password hashing wants to be SLOW. A general-purpose
	// hash is fast by design, which is exactly wrong here — it lets an attacker
	// with the dump try billions of candidates per second.
	hash, err := bcrypt.GenerateFromPassword([]byte(in.Password), s.cfg.BcryptCost)
	if err != nil {
		return nil, fmt.Errorf("hashing password: %w", err)
	}

	u := &models.User{
		Email:        email,
		PasswordHash: string(hash),
		Name:         strings.TrimSpace(in.Name),
		Role:         role,
	}
	if err := s.store.Users().Create(ctx, u); err != nil {
		return nil, err
	}
	return u, nil
}

// Login verifies credentials and returns a signed token.
//
// Both the "no such user" and "wrong password" paths return the same error and
// do comparable work: replying "no account with that email" faster than "wrong
// password" turns login into an account-enumeration oracle.
func (s *AuthService) Login(ctx context.Context, c Credentials) (string, *models.User, error) {
	email := strings.TrimSpace(strings.ToLower(c.Email))

	u, err := s.store.Users().GetByEmail(ctx, email)
	if err != nil {
		if errors.Is(err, models.ErrNotFound) {
			// Spend roughly the same time as a real comparison would.
			_ = bcrypt.CompareHashAndPassword(dummyHash, []byte(c.Password))
			return "", nil, models.ErrUnauthorized
		}
		return "", nil, err
	}

	if err := bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(c.Password)); err != nil {
		return "", nil, models.ErrUnauthorized
	}

	token, err := s.Issue(u)
	if err != nil {
		return "", nil, err
	}
	return token, u, nil
}

func (s *AuthService) Issue(u *models.User) (string, error) {
	now := time.Now().UTC()
	claims := Claims{
		UserID: u.ID,
		Role:   u.Role,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   fmt.Sprint(u.ID),
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(s.cfg.TokenTTL)),
			Issuer:    "order-processing",
		},
	}

	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := tok.SignedString([]byte(s.cfg.JWTSecret))
	if err != nil {
		return "", fmt.Errorf("signing token: %w", err)
	}
	return signed, nil
}

// Verify parses and validates a token.
//
// The algorithm check is not optional. Without it a token with alg=none, or one
// signed with HMAC using the public key of an RSA keypair, can be accepted as
// valid — the classic JWT vulnerability. jwt.WithValidMethods enforces that the
// token's algorithm is the one we actually issue.
func (s *AuthService) Verify(tokenString string) (*Claims, error) {
	var claims Claims

	_, err := jwt.ParseWithClaims(tokenString, &claims,
		func(t *jwt.Token) (any, error) { return []byte(s.cfg.JWTSecret), nil },
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithIssuer("order-processing"),
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", models.ErrUnauthorized, err)
	}
	return &claims, nil
}

func (s *AuthService) GetUser(ctx context.Context, id uint64) (*models.User, error) {
	return s.store.Users().GetByID(ctx, id)
}

func (s *AuthService) UpdateUser(ctx context.Context, id uint64, name, email string) (*models.User, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("%w: name is required", models.ErrInvalidInput)
	}
	return s.store.Users().Update(ctx, id, strings.TrimSpace(name), strings.ToLower(strings.TrimSpace(email)))
}

// dummyHash is a real bcrypt hash of a random string, used to equalise timing on
// the unknown-email path.
var dummyHash = []byte("$2a$12$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy")
