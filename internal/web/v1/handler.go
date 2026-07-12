// Package v1 implements checkout's HTTP transport: Variant A collection-noun
// routes under /checkout/v1/private/checkout/sessions (naming convention
// v3.0.1 — checkout, like auth, is a process-named service with no natural
// plural, so it uses the literal `checkout` segment and nests its resources
// beneath it). Handlers
// validate, call the logic layer, and translate domain errors to the shared
// httpx envelope. All routes are private: Kong edge-JWT pre-filters, the
// in-service authmw verification is authoritative, and sessions are
// owner-scoped by the JWT user_id (anti-IDOR).
package v1

import (
	"context"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/duynhlab/pkg/authmw"
	"github.com/duynhlab/pkg/httpx"

	"github.com/duynhlab/checkout-service/internal/core/domain"
	logicv1 "github.com/duynhlab/checkout-service/internal/logic/v1"
	"github.com/duynhlab/checkout-service/middleware"
)

// msgInvalidRequestBody is the shared 400 message for malformed JSON.
const msgInvalidRequestBody = "Invalid request body"

// Handler serves the checkout session API.
type Handler struct {
	svc *logicv1.CheckoutService
}

// NewHandler wires the handler over the logic layer.
func NewHandler(svc *logicv1.CheckoutService) *Handler {
	return &Handler{svc: svc}
}

// RegisterRoutes mounts the session routes on the private group. The caller
// passes the JWT middleware so tests can inject a fake.
func RegisterRoutes(r gin.IRouter, h *Handler, jwtMW gin.HandlerFunc) {
	private := r.Group("/checkout/v1/private/checkout", jwtMW)
	{
		private.POST("/sessions", h.CreateSession)
		private.GET("/sessions/:id", h.GetSession)
		private.PUT("/sessions/:id/address", h.SetAddress)
		private.PUT("/sessions/:id/shipping", h.SetShipping)
		private.PUT("/sessions/:id/payment", h.SetPayment)
		private.POST("/sessions/:id/confirm", h.ConfirmSession)
		private.DELETE("/sessions/:id", h.CancelSession)
	}
}

// CreateSession handles POST /checkout/v1/private/checkout/sessions — snapshot the
// cart, re-validate prices against product, return 201 (created) or 200 (an
// active session already exists; POST is idempotent).
func (h *Handler) CreateSession(c *gin.Context) {
	ctx, span := middleware.StartSpan(c.Request.Context(), "http.request", trace.WithAttributes(
		attribute.String("layer", "web"),
		attribute.String("method", c.Request.Method),
		attribute.String("path", c.Request.URL.Path),
	))
	defer span.End()
	logger := middleware.GetLoggerFromGinContext(c)

	session, created, err := h.svc.CreateSession(ctx, c.GetString(authmw.CtxUserID))
	if err != nil {
		span.RecordError(err)
		switch {
		case errors.Is(err, logicv1.ErrEmptyCart):
			httpx.RespondError(c, http.StatusConflict, httpx.CodeConflict, "Cart is empty")
		case errors.Is(err, logicv1.ErrUpstream):
			logger.Error("Session create upstream failure", zap.Error(err))
			httpx.RespondError(c, http.StatusInternalServerError, httpx.CodeInternal, "Internal server error")
		default:
			logger.Error("Session create failed", zap.Error(err))
			httpx.RespondError(c, http.StatusInternalServerError, httpx.CodeInternal, "Internal server error")
		}
		return
	}

	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	logger.Info("Checkout session ready",
		zap.String("session_id", session.ID), zap.Bool("created", created))
	c.JSON(status, toSessionResponse(session))
}

// GetSession handles GET /checkout/v1/private/checkout/sessions/:id.
func (h *Handler) GetSession(c *gin.Context) {
	ctx, span := middleware.StartSpan(c.Request.Context(), "http.request", trace.WithAttributes(
		attribute.String("layer", "web"),
		attribute.String("method", c.Request.Method),
		attribute.String("path", c.Request.URL.Path),
	))
	defer span.End()

	session, err := h.svc.GetSession(ctx, c.GetString(authmw.CtxUserID), c.Param("id"))
	if err != nil {
		h.respondSessionError(c, span, err)
		return
	}
	c.JSON(http.StatusOK, toSessionResponse(session))
}

// SetAddress handles PUT /checkout/v1/private/checkout/sessions/:id/address.
func (h *Handler) SetAddress(c *gin.Context) {
	ctx, span := middleware.StartSpan(c.Request.Context(), "http.request", trace.WithAttributes(
		attribute.String("layer", "web"),
		attribute.String("method", c.Request.Method),
		attribute.String("path", c.Request.URL.Path),
	))
	defer span.End()

	var addr addressRequest
	if err := c.ShouldBindJSON(&addr); err != nil {
		span.RecordError(err)
		httpx.RespondError(c, http.StatusBadRequest, httpx.CodeValidation, msgInvalidRequestBody)
		return
	}

	session, err := h.svc.SetAddress(ctx, c.GetString(authmw.CtxUserID), c.Param("id"), addr.toDomain())
	if err != nil {
		h.respondSessionError(c, span, err)
		return
	}
	c.JSON(http.StatusOK, toSessionResponse(session))
}

// SetShipping handles PUT /checkout/v1/private/checkout/sessions/:id/shipping.
// P2 records the method with a zero fee/tax stub (GetQuote lands in P3).
func (h *Handler) SetShipping(c *gin.Context) {
	var req shippingRequest
	h.updateSession(c, &req, func(ctx context.Context, userID, id string) (*domain.Session, error) {
		return h.svc.SetShipping(ctx, userID, id, req.ShippingMethod)
	})
}

// SetPayment handles PUT /checkout/v1/private/checkout/sessions/:id/payment.
// Only opaque tok_ references are accepted; PAN-like input is 400 before any
// persistence (the order/payment PCI-shaped rule).
func (h *Handler) SetPayment(c *gin.Context) {
	var req paymentRequest
	h.updateSession(c, &req, func(ctx context.Context, userID, id string) (*domain.Session, error) {
		return h.svc.SetPayment(ctx, userID, id, req.PaymentMethodToken)
	})
}

// updateSession is the shared bind → call → respond shape of the PUT steps.
func (h *Handler) updateSession(c *gin.Context, req any, call func(ctx context.Context, userID, id string) (*domain.Session, error)) {
	ctx, span := middleware.StartSpan(c.Request.Context(), "http.request", trace.WithAttributes(
		attribute.String("layer", "web"),
		attribute.String("method", c.Request.Method),
		attribute.String("path", c.Request.URL.Path),
	))
	defer span.End()

	if err := c.ShouldBindJSON(req); err != nil {
		span.RecordError(err)
		httpx.RespondError(c, http.StatusBadRequest, httpx.CodeValidation, msgInvalidRequestBody)
		return
	}

	session, err := call(ctx, c.GetString(authmw.CtxUserID), c.Param("id"))
	if err != nil {
		h.respondSessionError(c, span, err)
		return
	}
	c.JSON(http.StatusOK, toSessionResponse(session))
}

// maxIdempotencyKeyLen caps the client key so the composed order-side key
// ("checkout:<uuid>:<key>") always fits order's 200-char limit.
const maxIdempotencyKeyLen = 120

// ConfirmSession handles POST …/sessions/:id/confirm — the idempotent order
// handoff. The Idempotency-Key header is REQUIRED: the SPA generates one per
// checkout attempt and persists it so a retry always converges.
func (h *Handler) ConfirmSession(c *gin.Context) {
	ctx, span := middleware.StartSpan(c.Request.Context(), "http.request", trace.WithAttributes(
		attribute.String("layer", "web"),
		attribute.String("method", c.Request.Method),
		attribute.String("path", c.Request.URL.Path),
	))
	defer span.End()
	logger := middleware.GetLoggerFromGinContext(c)

	key := c.GetHeader("Idempotency-Key")
	if key == "" {
		httpx.RespondError(c, http.StatusBadRequest, httpx.CodeIdempotencyKeyRequired, "Idempotency-Key header is required")
		return
	}
	if len(key) > maxIdempotencyKeyLen {
		httpx.RespondError(c, http.StatusBadRequest, httpx.CodeValidation, "Idempotency-Key too long (max 120 chars)")
		return
	}

	session, err := h.svc.Confirm(ctx, c.GetString(authmw.CtxUserID), c.Param("id"), key)
	if err != nil {
		span.RecordError(err)
		switch {
		case errors.Is(err, logicv1.ErrPriceChanged):
			c.JSON(http.StatusConflict, gin.H{
				"error":   gin.H{"code": httpx.CodePriceChanged, "message": "Prices changed; session requoted — review and confirm again"},
				"session": toSessionResponse(session),
			})
		case errors.Is(err, logicv1.ErrStockUnavailable):
			c.JSON(http.StatusConflict, gin.H{
				"error":   gin.H{"code": httpx.CodeStockUnavailable, "message": "Some items are no longer available"},
				"session": toSessionResponse(session),
			})
		case errors.Is(err, logicv1.ErrConfirmInFlight):
			httpx.RespondError(c, http.StatusConflict, httpx.CodeConflict, "A confirm is already in flight for this session")
		case errors.Is(err, logicv1.ErrKeyConflict):
			httpx.RespondError(c, http.StatusConflict, httpx.CodeIdempotencyConflict, "Idempotency-Key was used for a different request")
		case errors.Is(err, logicv1.ErrUpstream):
			c.Header("Retry-After", "2")
			logger.Error("Confirm upstream failure", zap.Error(err))
			httpx.RespondError(c, http.StatusServiceUnavailable, httpx.CodeInternal, "Confirm temporarily unavailable, retry with the same Idempotency-Key")
		default:
			h.respondSessionError(c, span, err)
		}
		return
	}

	logger.Info("Checkout confirmed",
		zap.String("session_id", session.ID), zap.String("order_id", session.OrderID))
	c.JSON(http.StatusCreated, toSessionResponse(session))
}

// CancelSession handles DELETE /checkout/v1/private/checkout/sessions/:id.
func (h *Handler) CancelSession(c *gin.Context) {
	ctx, span := middleware.StartSpan(c.Request.Context(), "http.request", trace.WithAttributes(
		attribute.String("layer", "web"),
		attribute.String("method", c.Request.Method),
		attribute.String("path", c.Request.URL.Path),
	))
	defer span.End()

	if err := h.svc.Cancel(ctx, c.GetString(authmw.CtxUserID), c.Param("id")); err != nil {
		h.respondSessionError(c, span, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "Session cancelled"})
}

// respondSessionError maps logic errors to the shared envelope. Unknown and
// foreign sessions are both 404 (anti-IDOR); an elapsed TTL is 410
// SESSION_EXPIRED (the session existed — distinct from 404).
func (h *Handler) respondSessionError(c *gin.Context, span trace.Span, err error) {
	span.RecordError(err)
	switch {
	case errors.Is(err, logicv1.ErrInvalidPaymentToken):
		// Generic message by design: the rejected value must never be echoed
		// (it may be PAN-shaped).
		httpx.RespondError(c, http.StatusBadRequest, httpx.CodeValidation, "payment_method_token must be an opaque tok_ reference")
	case errors.Is(err, logicv1.ErrSessionNotFound):
		httpx.RespondError(c, http.StatusNotFound, httpx.CodeNotFound, "Checkout session not found")
	case errors.Is(err, logicv1.ErrSessionExpired):
		httpx.RespondError(c, http.StatusGone, httpx.CodeSessionExpired, "Checkout session expired")
	case errors.Is(err, logicv1.ErrInvalidTransition):
		httpx.RespondError(c, http.StatusConflict, httpx.CodeInvalidTransition, "Session state does not allow this operation")
	case errors.Is(err, domain.ErrStaleTransition):
		httpx.RespondError(c, http.StatusConflict, httpx.CodeConflict, "Session was modified concurrently; reload and retry")
	default:
		middleware.GetLoggerFromGinContext(c).Error("Session operation failed", zap.Error(err))
		httpx.RespondError(c, http.StatusInternalServerError, httpx.CodeInternal, "Internal server error")
	}
}

// shippingRequest is the PUT …/shipping payload.
type shippingRequest struct {
	ShippingMethod string `json:"shipping_method" binding:"required,max=40"`
}

// paymentRequest is the PUT …/payment payload. Shape (tok_ only, no PAN) is
// enforced in the logic layer so the rule cannot drift per transport.
type paymentRequest struct {
	PaymentMethodToken string `json:"payment_method_token" binding:"required,max=64"`
}

// addressRequest is the PUT …/address payload; snake_case per the platform
// JSON convention, validated at this boundary.
type addressRequest struct {
	FullName string `json:"full_name" binding:"required,max=120"`
	Line1    string `json:"line1" binding:"required,max=200"`
	Line2    string `json:"line2" binding:"max=200"`
	City     string `json:"city" binding:"required,max=100"`
	Region   string `json:"region" binding:"max=100"`
	PostCode string `json:"post_code" binding:"max=20"`
	Country  string `json:"country" binding:"required,max=56"`
}

func (a *addressRequest) toDomain() *domain.Address {
	return &domain.Address{
		FullName: a.FullName,
		Line1:    a.Line1,
		Line2:    a.Line2,
		City:     a.City,
		Region:   a.Region,
		PostCode: a.PostCode,
		Country:  a.Country,
	}
}
