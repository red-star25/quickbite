package inventory

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	inventoryv1 "github.com/red-star25/quickbite/proto/gen/go/inventory/v1"
)

type Client struct {
	conn *grpc.ClientConn
	api  inventoryv1.InventoryServiceClient
}

// New creates a new inventory client.
func New(addr string) (*Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Create a new gRPC connection to the inventory service.
	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, err
	}

	return &Client{
		conn: conn,
		api:  inventoryv1.NewInventoryServiceClient(conn),
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Reserve(ctx context.Context, sku string, qty int32) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	_, err := c.api.ReserveStock(ctx, &inventoryv1.ReserveStockRequest{
		Sku:      sku,
		Quantity: qty,
	})
	return err
}
