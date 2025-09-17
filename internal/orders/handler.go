package orders

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"time"
	"wb_tech/L0/internal/cacher"
	"wb_tech/L0/internal/handlers"

	"github.com/julienschmidt/httprouter"
)

// P.S. Write through strategy для кеша

const (
	getOrderURL    = "/order"
	createOrderURL = "/create_order"
	indexURL       = "/"
	cacheTTL       = 10 * time.Minute
)

type handler struct {
	repository Repository
	cacher     cacher.Cacher
	templates  *template.Template
}

type TemplateData struct {
	Order   *Order
	Error   string
	Success string
	Query   string
}

func NewHandler(repository Repository, cacher cacher.Cacher) handlers.Handler {
	// Загружаем шаблоны
	templates := template.Must(template.ParseFiles(
		"templates/order.html",        // главная страница с формой
		"templates/order_result.html", // шаблон для результатов
	))

	return &handler{
		repository: repository,
		cacher:     cacher,
		templates:  templates,
	}
}

func (h *handler) Register(router *httprouter.Router) {
	router.HandlerFunc(http.MethodGet, getOrderURL, h.GetOrder)
	router.HandlerFunc(http.MethodPost, createOrderURL, h.CreateOrder)
	router.HandlerFunc(http.MethodGet, indexURL, h.Index)
}

func (h *handler) Index(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	data := TemplateData{
		Query: r.URL.Query().Get("uid"), // сохраняем предыдущий ввод
	}

	if err := h.templates.ExecuteTemplate(w, "order.html", data); err != nil {
		http.Error(w, "Error rendering template: "+err.Error(), http.StatusInternalServerError)
	}
}

func (h *handler) GetOrder(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	orderUID := r.URL.Query().Get("uid")
	if orderUID == "" {
		h.renderTemplate(w, "order.html", TemplateData{
			Error: "UID заказа обязателен для заполнения",
		})
		return
	}

	if r.Header.Get("Accept") == "application/json" {
		h.handleJSONRequest(ctx, w, orderUID)
		return
	}

	h.handleHTMLRequest(ctx, w, orderUID)
}

func (h *handler) handleJSONRequest(ctx context.Context, w http.ResponseWriter, orderUID string) {
	order, err := h.getFromCache(ctx, orderUID)
	if err == nil && order != nil {
		h.writeJSONResponse(w, order)
		return
	}

	order, err = h.repository.GetOrderByUID(ctx, orderUID)
	if err != nil {
		http.Error(w, fmt.Sprintf("order not found: %v", err), http.StatusNotFound)
		return
	}

	if order == nil {
		http.Error(w, "order not found", http.StatusNotFound)
		return
	}

	if err := h.cacheOrder(ctx, orderUID, order); err != nil {
		log.Printf("Warning: failed to cache order %s: %v\n", orderUID, err)
	}

	h.writeJSONResponse(w, order)
}

func (h *handler) handleHTMLRequest(ctx context.Context, w http.ResponseWriter, orderUID string) {
	order, err := h.getFromCache(ctx, orderUID)
	if err != nil {
		log.Printf("Cache miss for %s: %v\n", orderUID, err)
	}

	if order == nil {
		order, err = h.repository.GetOrderByUID(ctx, orderUID)
		if err != nil {
			h.renderTemplate(w, "order.html", TemplateData{
				Error: fmt.Sprintf("Заказ не найден: %v", err),
				Query: orderUID,
			})
			return
		}

		if order == nil {
			h.renderTemplate(w, "order.html", TemplateData{
				Error: "Заказ с указанным UID не найден",
				Query: orderUID,
			})
			return
		}

		if err := h.cacheOrder(ctx, orderUID, order); err != nil {
			log.Printf("Warning: failed to cache order %s: %v\n", orderUID, err)
		}
	}

	h.renderTemplate(w, "order_result.html", TemplateData{
		Order:   order,
		Success: "Заказ найден!",
		Query:   orderUID,
	})
}

func (h *handler) getFromCache(ctx context.Context, orderUID string) (*Order, error) {
	cachedData, err := h.cacher.Get(ctx, orderUID)
	if err != nil {
		return nil, err
	}

	if cachedData == "" {
		return nil, fmt.Errorf("empty cache data")
	}

	var order Order
	if err := json.Unmarshal([]byte(cachedData), &order); err != nil {
		h.cacher.Invalidate(ctx, orderUID)
		return nil, fmt.Errorf("invalid cache data: %v", err)
	}

	return &order, nil
}

func (h *handler) cacheOrder(ctx context.Context, orderUID string, order *Order) error {
	orderJSON, err := json.Marshal(order)
	if err != nil {
		return fmt.Errorf("failed to marshal order: %v", err)
	}

	return h.cacher.Set(ctx, orderUID, orderJSON, cacheTTL)
}

func (h *handler) writeJSONResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
	}
}

func (h *handler) renderTemplate(w http.ResponseWriter, templateName string, data TemplateData) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	if err := h.templates.ExecuteTemplate(w, templateName, data); err != nil {
		http.Error(w, "Error rendering template: "+err.Error(), http.StatusInternalServerError)
	}
}

func (h *handler) CreateOrder(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var order Order
	if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	if err := h.repository.CreateOrder(ctx, &order); err != nil {
		http.Error(w, fmt.Sprintf("failed to create order: %v", err), http.StatusInternalServerError)
		return
	}

	if err := h.cacheOrder(ctx, order.OrderUID, &order); err != nil {
		log.Printf("Warning: failed to cache new order %s: %v\n", order.OrderUID, err)
	}

	w.WriteHeader(http.StatusCreated)
	h.writeJSONResponse(w, order)
}
