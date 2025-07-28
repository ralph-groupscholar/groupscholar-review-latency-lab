package main

import "testing"

func nearlyEqual(a, b, tol float64) bool {
	if a > b {
		return a-b <= tol
	}
	return b-a <= tol
}

func TestLinearTrendIncreasing(t *testing.T) {
	values := []int{1, 2, 3, 4, 5, 6, 7}
	slope, r2 := linearTrend(values)
	if slope <= 0 {
		t.Fatalf("expected positive slope, got %.3f", slope)
	}
	if r2 < 0.95 {
		t.Fatalf("expected high r2, got %.3f", r2)
	}
}

func TestLinearTrendDecreasing(t *testing.T) {
	values := []int{9, 7, 5, 3, 1}
	slope, _ := linearTrend(values)
	if slope >= 0 {
		t.Fatalf("expected negative slope, got %.3f", slope)
	}
}

func TestLinearTrendFlat(t *testing.T) {
	values := []int{4, 4, 4, 4, 4}
	slope, r2 := linearTrend(values)
	if !nearlyEqual(slope, 0, 1e-6) {
		t.Fatalf("expected zero slope, got %.6f", slope)
	}
	if !nearlyEqual(r2, 0, 1e-6) {
		t.Fatalf("expected zero r2 for flat series, got %.6f", r2)
	}
}

func TestClassifyTrend(t *testing.T) {
	cases := []struct {
		slope    float64
		expected string
	}{
		{0.7, "Increasing"},
		{-0.8, "Decreasing"},
		{0.2, "Flat"},
		{-0.2, "Flat"},
	}
	for _, tc := range cases {
		if got := classifyTrend(tc.slope); got != tc.expected {
			t.Fatalf("slope %.2f expected %s got %s", tc.slope, tc.expected, got)
		}
	}
}
