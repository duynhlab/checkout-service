// Package webv1 exposes the checkout HTTP API (v1).
package webv1

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/duynhlab/checkout-service/config"
	logicv1 "github.com/duynhlab/checkout-service/internal/logic/v1"
)

// Handler wires HTTP transport to the checkout business logic.
type Handler struct {
	svc *logicv1.CheckoutService
	cfg *config.Config
}

// NewHandler creates the v1 HTTP handler.
func NewHandler(svc *logicv1.CheckoutService, cfg *config.Config) *Handler {
	return &Handler{svc: svc, cfg: cfg}
}

// RegisterRoutes mounts the v1 API on the router.
func (h *Handler) RegisterRoutes(r *gin.Engine) {
	v1 := r.Group("/api/v1")
	v1.POST("/checkout", h.quote)
	v1.GET("/info", h.info)
}

// quoteRequest is the POST /api/v1/checkout payload.
type quoteRequest struct {
	Items []logicv1.Item `json:"items" binding:"required"`
}

// quote prices a cart.
func (h *Handler) quote(c *gin.Context) {
	var req quoteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
		return
	}

	q, err := h.svc.Quote(req.Items)
	if err != nil {
		switch {
		case errors.Is(err, logicv1.ErrEmptyCart), errors.Is(err, logicv1.ErrInvalidItem):
			c.JSON(http.StatusUnprocessableEntity, gin.H{"error": err.Error()})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		}
		return
	}

	c.JSON(http.StatusOK, q)
}

// info reports the service identity and effective runtime configuration —
// the fastest way to confirm which env/version a pod is running.
// Secrets never appear here.
func (h *Handler) info(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"service": h.cfg.Service.Name,
		"version": h.cfg.Service.Version,
		"env":     h.cfg.Service.Env,
		"logging": gin.H{
			"level":  h.cfg.Logging.Level,
			"format": h.cfg.Logging.Format,
		},
		"tracing": gin.H{
			"enabled":     h.cfg.Tracing.Enabled,
			"sample_rate": h.cfg.Tracing.SampleRate,
		},
		"profiling": gin.H{"enabled": h.cfg.Profiling.Enabled},
		"metrics":   gin.H{"enabled": h.cfg.Metrics.Enabled, "path": h.cfg.Metrics.Path},
	})
}
