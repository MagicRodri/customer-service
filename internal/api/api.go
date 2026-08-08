package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/MagicRodri/customer-service/internal/app"
	"github.com/MagicRodri/customer-service/internal/domain"
)

type API struct {
	app *app.App
	log *slog.Logger
}

func New(a *app.App, log *slog.Logger) *API { return &API{app: a, log: log} }

func (a *API) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", a.health)
	mux.HandleFunc("POST /customers", a.createCustomer)
	mux.HandleFunc("GET /customers", a.listCustomers)
	mux.HandleFunc("GET /customers/{id}", a.getCustomer)
	mux.HandleFunc("POST /customers/{id}/block", a.blockCustomer)
	mux.HandleFunc("POST /customers/{id}/unblock", a.unblockCustomer)
	return a.logRequests(mux)
}

func (a *API) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) createCustomer(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Email string `json:"email"`
		Name  string `json:"name"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	customer, err := a.app.CreateCustomer(r.Context(), req.Email, req.Name)
	if err != nil {
		a.fail(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, customer)
}

func (a *API) listCustomers(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	customers, err := a.app.ListCustomers(r.Context(), limit)
	if err != nil {
		a.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"customers": customers})
}

func (a *API) getCustomer(w http.ResponseWriter, r *http.Request) {
	customer, err := a.app.GetCustomer(r.Context(), r.PathValue("id"))
	if err != nil {
		a.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, customer)
}

func (a *API) blockCustomer(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Reason string `json:"reason"`
	}
	// A body is optional here, so a decode failure is not fatal.
	_ = decodeJSON(r, &req)

	customer, err := a.app.BlockCustomer(r.Context(), r.PathValue("id"), req.Reason)
	if err != nil {
		a.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, customer)
}

func (a *API) unblockCustomer(w http.ResponseWriter, r *http.Request) {
	customer, err := a.app.UnblockCustomer(r.Context(), r.PathValue("id"))
	if err != nil {
		a.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, customer)
}

func (a *API) fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, err)
	case errors.Is(err, domain.ErrInvalidInput):
		writeError(w, http.StatusBadRequest, err)
	case errors.Is(err, domain.ErrEmailTaken), errors.Is(err, domain.ErrAlreadyInThat):
		writeError(w, http.StatusConflict, err)
	default:
		a.log.Error("request failed", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("internal error"))
	}
}

func (a *API) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a.log.Debug("request", "method", r.Method, "path", r.URL.Path)
		next.ServeHTTP(w, r)
	})
}

func decodeJSON(r *http.Request, dst any) error {
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return errors.New("invalid JSON body")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
