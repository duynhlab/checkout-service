// Package clients adapts the generated gRPC stubs (cart.v1, product.v1,
// order.v1) to the logic layer's ports. Transport-only: no business rules
// live here.
package clients

import (
	"context"

	"google.golang.org/grpc"

	cartv1 "github.com/duynhlab/pkg/proto/cart/v1"
	orderv1 "github.com/duynhlab/pkg/proto/order/v1"
	productv1 "github.com/duynhlab/pkg/proto/product/v1"

	"github.com/duynhlab/checkout-service/internal/core/domain"
	logicv1 "github.com/duynhlab/checkout-service/internal/logic/v1"
)

// CartClient satisfies logicv1.CartFetcher over cart.v1/GetCart.
type CartClient struct {
	c cartv1.CartServiceClient
}

// NewCartClient wraps an established gRPC connection.
func NewCartClient(conn grpc.ClientConnInterface) *CartClient {
	return &CartClient{c: cartv1.NewCartServiceClient(conn)}
}

// GetCart fetches the user's cart lines.
func (c *CartClient) GetCart(ctx context.Context, userID string) ([]logicv1.CartLine, error) {
	resp, err := c.c.GetCart(ctx, &cartv1.GetCartRequest{UserId: userID})
	if err != nil {
		return nil, err
	}
	lines := make([]logicv1.CartLine, 0, len(resp.GetItems()))
	for _, it := range resp.GetItems() {
		lines = append(lines, logicv1.CartLine{
			ProductID:      it.GetProductId(),
			ProductName:    it.GetProductName(),
			Quantity:       int(it.GetQuantity()),
			CartPriceMinor: it.GetCartPriceMinor(),
		})
	}
	return lines, nil
}

// ProductClient satisfies logicv1.ProductFetcher over product.v1/GetProducts.
type ProductClient struct {
	c productv1.ProductServiceClient
}

// NewProductClient wraps an established gRPC connection.
func NewProductClient(conn grpc.ClientConnInterface) *ProductClient {
	return &ProductClient{c: productv1.NewProductServiceClient(conn)}
}

// OrderClient satisfies logicv1.OrderCreator over order.v1/CreateOrder — the
// confirm handoff (RFC-0015 P2, ADR-018). The call is idempotent on the order
// side by (user_id, idempotency_key), so retrying after a transport error can
// never double-create.
type OrderClient struct {
	c orderv1.OrderServiceClient
}

// NewOrderClient wraps an established gRPC connection.
func NewOrderClient(conn grpc.ClientConnInterface) *OrderClient {
	return &OrderClient{c: orderv1.NewOrderServiceClient(conn)}
}

// CreateOrder places the order from the session's validated snapshot.
func (c *OrderClient) CreateOrder(ctx context.Context, userID string, items []domain.SessionItem, paymentToken, idemKey string) (string, string, error) {
	reqItems := make([]*orderv1.OrderItem, 0, len(items))
	for _, it := range items {
		reqItems = append(reqItems, &orderv1.OrderItem{
			ProductId:      it.ProductID,
			ProductName:    it.ProductName,
			Quantity:       int32(it.Quantity),
			UnitPriceMinor: it.UnitPriceMinor,
		})
	}
	resp, err := c.c.CreateOrder(ctx, &orderv1.CreateOrderRequest{
		UserId:         userID,
		Items:          reqItems,
		PaymentMethod:  paymentToken,
		IdempotencyKey: idemKey,
	})
	if err != nil {
		return "", "", err
	}
	return resp.GetOrderId(), resp.GetStatus(), nil
}

// GetProducts fetches the authoritative price/availability batch.
func (c *ProductClient) GetProducts(ctx context.Context, ids []string) ([]logicv1.ProductInfo, error) {
	resp, err := c.c.GetProducts(ctx, &productv1.GetProductsRequest{ProductIds: ids})
	if err != nil {
		return nil, err
	}
	infos := make([]logicv1.ProductInfo, 0, len(resp.GetProducts()))
	for _, p := range resp.GetProducts() {
		infos = append(infos, logicv1.ProductInfo{
			ProductID:      p.GetProductId(),
			Name:           p.GetName(),
			UnitPriceMinor: p.GetPriceMinor(),
			AvailableQty:   int(p.GetAvailableQty()),
		})
	}
	return infos, nil
}
