package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"

	"quickbite/orders/internal/store"
)

type Server struct {
	store *store.OrdersStore
}

func NewServer(store *store.OrdersStore) *Server {
	return &Server{store: store}
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
	UserID string `json:"userId"`
	Note   string `json:"note"`
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
	if req.UserID == "" {
		http.Error(w, "userId is required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	id, err := s.store.Create(ctx, req.UserID, req.Note)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusCreated, createOrderResponse{ID: id})
}

type getOrderResponse struct {
	ID        int64  `json:"id"`
	UserID    string `json:"userId"`
	Note      string `json:"note"`
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
		CreatedAt: o.CreatedAt.UTC().Format(time.RFC3339),
	}
	writeJSON(w, http.StatusOK, resp)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
