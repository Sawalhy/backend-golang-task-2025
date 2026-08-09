package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/Sawalhy/backend-golang-task-2025/internal/api/middleware"
	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/internal/services"
)

type AuthHandler struct {
	auth *services.AuthService
}

func NewAuthHandler(auth *services.AuthService) *AuthHandler {
	return &AuthHandler{auth: auth}
}

// Binding tags do the first pass of validation, so malformed input never reaches
// a service. They are not the last line of defence — the services re-check what
// matters, because a worker calling the same service has no Gin binding at all.
type createUserRequest struct {
	Email    string `json:"email"    binding:"required,email"`
	Password string `json:"password" binding:"required,min=8,max=72"`
	Name     string `json:"name"     binding:"required,min=1,max=200"`
	// Role is accepted only when the caller is already an admin; see below.
	Role string `json:"role" binding:"omitempty,oneof=CUSTOMER ADMIN"`
}

type loginRequest struct {
	Email    string `json:"email"    binding:"required,email"`
	Password string `json:"password" binding:"required"`
}

type loginResponse struct {
	Token string       `json:"token"`
	User  *models.User `json:"user"`
}

// CreateUser handles POST /api/v1/users.
//
//	@Summary	Register a user
//	@Tags		users
//	@Accept		json
//	@Produce	json
//	@Param		body	body		createUserRequest	true	"user"
//	@Success	201		{object}	models.User
//	@Failure	400,409	{object}	ErrorResponse
//	@Router		/users [post]
func (h *AuthHandler) CreateUser(c *gin.Context) {
	var req createUserRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, wrapBinding(err))
		return
	}

	// Self-registration cannot mint an admin. Without this check the role field
	// is a privilege escalation: anyone who can POST /users can post
	// "role":"ADMIN" and reach every admin route.
	role := models.RoleCustomer
	if req.Role == string(models.RoleAdmin) && middleware.IsAdmin(c) {
		role = models.RoleAdmin
	}

	u, err := h.auth.Register(c.Request.Context(), services.RegisterInput{
		Email:    req.Email,
		Password: req.Password,
		Name:     req.Name,
		Role:     role,
	})
	if err != nil {
		fail(c, err)
		return
	}

	c.JSON(http.StatusCreated, u)
}

// Login handles POST /api/v1/auth/login.
//
// Not in the spec: README.md mandates JWT authentication but defines no endpoint
// that issues a token. Flagged in the README rather than silently added.
//
//	@Summary	Log in and receive a JWT
//	@Tags		auth
//	@Accept		json
//	@Produce	json
//	@Param		body	body		loginRequest	true	"credentials"
//	@Success	200		{object}	loginResponse
//	@Failure	400,401	{object}	ErrorResponse
//	@Router		/auth/login [post]
func (h *AuthHandler) Login(c *gin.Context) {
	var req loginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, wrapBinding(err))
		return
	}

	token, user, err := h.auth.Login(c.Request.Context(), services.Credentials{
		Email:    req.Email,
		Password: req.Password,
	})
	if err != nil {
		fail(c, err)
		return
	}

	c.JSON(http.StatusOK, loginResponse{Token: token, User: user})
}

// GetUser handles GET /api/v1/users/{id}.
//
//	@Summary	Get a user profile
//	@Tags		users
//	@Produce	json
//	@Param		id	path		int	true	"user id"
//	@Success	200	{object}	models.User
//	@Failure	403,404	{object}	ErrorResponse
//	@Security	BearerAuth
//	@Router		/users/{id} [get]
func (h *AuthHandler) GetUser(c *gin.Context) {
	id, ok := idParam(c, "id")
	if !ok {
		return
	}

	// A customer may read only their own profile; an admin may read any.
	if !middleware.IsAdmin(c) && middleware.UserID(c) != id {
		fail(c, models.ErrForbidden)
		return
	}

	u, err := h.auth.GetUser(c.Request.Context(), id)
	if err != nil {
		fail(c, err)
		return
	}
	c.JSON(http.StatusOK, u)
}

type updateUserRequest struct {
	Name  string `json:"name"  binding:"required,min=1,max=200"`
	Email string `json:"email" binding:"required,email"`
}

// UpdateUser handles PUT /api/v1/users/{id}.
//
//	@Summary	Update a user profile
//	@Tags		users
//	@Accept		json
//	@Produce	json
//	@Param		id		path		int					true	"user id"
//	@Param		body	body		updateUserRequest	true	"profile"
//	@Success	200		{object}	models.User
//	@Failure	400,403,404,409	{object}	ErrorResponse
//	@Security	BearerAuth
//	@Router		/users/{id} [put]
func (h *AuthHandler) UpdateUser(c *gin.Context) {
	id, ok := idParam(c, "id")
	if !ok {
		return
	}
	if !middleware.IsAdmin(c) && middleware.UserID(c) != id {
		fail(c, models.ErrForbidden)
		return
	}

	var req updateUserRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, wrapBinding(err))
		return
	}

	u, err := h.auth.UpdateUser(c.Request.Context(), id, req.Name, req.Email)
	if err != nil {
		fail(c, err)
		return
	}
	c.JSON(http.StatusOK, u)
}
