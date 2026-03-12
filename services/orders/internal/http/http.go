package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"

	"github.com/red-star25/quickbite/orders/internal/kafkabus"
	"github.com/red-star25/quickbite/orders/internal/store"
)

type Server struct {
	store *store.OrdersStore
	bus   *kafkabus.Bus
}

func NewServer(store *store.OrdersStore, bus *kafkabus.Bus) *Server {
	return &Server{store: store, bus: bus}
}

func (s *Server) Routes() http.Handler {
	r := chi.NewRouter()

	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	r.Post("/v1/orders", s.handleCreateOrder)
	r.Get("/v1/orders/{id}", s.handleGetOrder)

	return r
}

type createOrderRequest struct {
	UserID   string `json:"userId"`
	Sku      string `json:"sku"`
	Quantity int    `json:"quantity"`
	Note     string `json:"note"`
}

type createOrderResponse struct {
	ID int64 `json:"id"`
}

func (s *Server) handleCreateOrder(w http.ResponseWriter, r *http.Request) {
	var req createOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	req.UserID = strings.TrimSpace(req.UserID)
	req.Sku = strings.TrimSpace(req.Sku)

	if req.UserID == "" {
		http.Error(w, "userId is required", http.StatusBadRequest)
		return
	}
	if req.Sku == "" {
		http.Error(w, "sku is required", http.StatusBadRequest)
		return
	}
	if req.Quantity <= 0 {
		http.Error(w, "quantity must be greater than 0", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	// Create a new order in the database.
	id, err := s.store.Create(ctx, req.UserID, req.Note, req.Sku, req.Quantity)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	evt := kafkabus.OrderCreated{
		EventID:  newEventID(),
		Type:     "OrderCreated",
		Time:     time.Now().UTC(),
		OrderID:  id,
		UserID:   req.UserID,
		Sku:      req.Sku,
		Quantity: req.Quantity,
	}

	if err := kafkabus.PublishOrderCreated(ctx, s.bus.OrderWriter, evt); err != nil {
		http.Error(w, "kafka publish failed", http.StatusServiceUnavailable)
		return
	}

	// =============================== INVENTORY RESERVE ===============================
	// // Once the order is created, we need to reserve the stock for the product.
	// err = s.inv.Reserve(ctx, req.Sku, int32(req.Quantity))
	// if err != nil {
	// 	_ = s.store.UpdateStatus(ctx, id, store.StatusCancelled)
	// 	// If the inventory reserve fails, we need to update the order status to cancelled.

	// 	st, ok := status.FromError(err)
	// 	log.Printf("inventory reserve error: ok=%v code=%v msg=%q err=%T %v", ok, st.Code(), st.Message(), err, err)
	// 	if ok {
	// 		switch st.Code() {
	// 		case codes.FailedPrecondition, codes.ResourceExhausted:
	// 			// insufficient stock / cannot reserve due to current state
	// 			http.Error(w, "not enough stock", http.StatusConflict) // 409
	// 			return
	// 		case codes.InvalidArgument:
	// 			http.Error(w, st.Message(), http.StatusBadRequest) // 400
	// 			return
	// 		case codes.DeadlineExceeded:
	// 			http.Error(w, "inventory timeout", http.StatusGatewayTimeout) // 504
	// 			return
	// 		case codes.Unavailable:
	// 			http.Error(w, "inventory unavailable", http.StatusServiceUnavailable) // 503
	// 			return
	// 		default:
	// 			http.Error(w, "inventory error", http.StatusInternalServerError)
	// 			return
	// 		}
	// 	}

	// 	http.Error(w, "inventory error", http.StatusInternalServerError)
	// 	return
	// }

	// Once the stock is reserved, we need to update the order status to confirmed.
	// _ = s.store.UpdateStatus(ctx, id, store.StatusConfirmed)
	// ===================================================================================

	writeJSON(w, http.StatusCreated, createOrderResponse{ID: id})
}

func newEventID() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

type getOrderResponse struct {
	ID        int64  `json:"id"`
	UserID    string `json:"userId"`
	Note      string `json:"note"`
	Status    string `json:"status"`
	Sku       string `json:"sku"`
	Quantity  int    `json:"quantity"`
	CreatedAt string `json:"createdAt"`
}

func (s *Server) handleGetOrder(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil || id <= 0 {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	o, err := s.store.GetByID(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	resp := getOrderResponse{
		ID:        o.ID,
		UserID:    o.UserID,
		Note:      o.Note,
		Status:    o.Status,
		Sku:       o.Sku,
		Quantity:  o.Quantity,
		CreatedAt: o.CreatedAt.UTC().Format(time.RFC3339),
	}
	writeJSON(w, http.StatusOK, resp)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
