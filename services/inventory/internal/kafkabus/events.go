package kafkabus

import "time"

type InventoryResult struct {
	EventID  string    `json:"eventId"`
	Type     string    `json:"type"` // InventoryReserved | InventoryRejected
	Time     time.Time `json:"time"`
	OrderID  int64     `json:"orderId"`
	Reserved bool      `json:"reserved"`
	Reason   string    `json:"reason,omitempty"`
}

type OrderCreated struct {
	EventID  string    `json:"eventId"`
	Type     string    `json:"type"` // OrderCreated
	Time     time.Time `json:"time"`
	OrderID  int64     `json:"orderId"`
	UserID   string    `json:"userId"`
	Sku      string    `json:"sku"`
	Quantity int       `json:"quantity"`
}
