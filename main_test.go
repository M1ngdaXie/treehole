package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// ───────────────────────── secret gate ─────────────────────────

func TestServeWs_WrongSecret(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/ws?secret=wrong", nil)
	w := httptest.NewRecorder()
	serveWs(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", w.Code)
	}
}

func TestServeWs_NoSecret(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/ws", nil)
	w := httptest.NewRecorder()
	serveWs(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", w.Code)
	}
}

func TestServeWs_CorrectSecret(t *testing.T) {
	// Correct secret should NOT return 403 (it will fail the WS upgrade
	// since it's not a real WebSocket request, but that's a different error).
	req := httptest.NewRequest(http.MethodGet, "/ws?secret=Mingda2026", nil)
	w := httptest.NewRecorder()
	serveWs(w, req)
	if w.Code == http.StatusForbidden {
		t.Fatalf("correct secret should not be rejected")
	}
}

// ───────────────────────── rate limiter ─────────────────────────

func TestLimiter_AllowsUpToBurst(t *testing.T) {
	l := newLimiter(0.5, 5) // burst=5, same as matchLimiter
	for i := 0; i < 5; i++ {
		if !l.allow("1.2.3.4") {
			t.Fatalf("request %d should be allowed (within burst)", i+1)
		}
	}
}

func TestLimiter_BlocksAfterBurst(t *testing.T) {
	l := newLimiter(0.5, 5)
	for i := 0; i < 5; i++ {
		l.allow("1.2.3.4")
	}
	if l.allow("1.2.3.4") {
		t.Fatal("request after burst should be blocked")
	}
}

func TestLimiter_DifferentIPsAreIndependent(t *testing.T) {
	l := newLimiter(0.5, 5)
	for i := 0; i < 5; i++ {
		l.allow("1.2.3.4")
	}
	// Different IP should still have a full bucket
	if !l.allow("9.9.9.9") {
		t.Fatal("different IP should not be affected by another IP's rate limit")
	}
}
