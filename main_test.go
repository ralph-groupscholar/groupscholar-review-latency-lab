package main

import "testing"

func TestComputePercentilesEmpty(t *testing.T) {
	result := computePercentiles([]int{}, []int{50, 90})
	if result["p50"] != 0 || result["p90"] != 0 {
		t.Fatalf("expected zeros for empty input, got %+v", result)
	}
}

func TestLinearTrendIncreasing(t *testing.T) {
	values := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	slope, r2 := linearTrend(values)
	if slope <= 0.4 {
		t.Fatalf("expected positive slope, got %.2f", slope)
	}
	if r2 <= 0.9 {
		t.Fatalf("expected strong fit, got %.2f", r2)
	}
}

func TestComputePercentilesSample(t *testing.T) {
	values := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	result := computePercentiles(values, []int{50, 90})
	if result["p50"] != 5 && result["p50"] != 6 {
		t.Fatalf("expected p50 near middle, got %d", result["p50"])
	}
	if result["p90"] < 9 {
		t.Fatalf("expected p90 near high end, got %d", result["p90"])
	}
}

func TestClassifyFlowBalance(t *testing.T) {
	if got := classifyFlowBalance(0.2); got != "Growing" {
		t.Fatalf("expected Growing, got %s", got)
	}
	if got := classifyFlowBalance(-0.2); got != "Draining" {
		t.Fatalf("expected Draining, got %s", got)
	}
	if got := classifyFlowBalance(0.05); got != "Stable" {
		t.Fatalf("expected Stable, got %s", got)
	}
}

func TestComputeStageAgingPercentiles(t *testing.T) {
	stage := &StageState{
		Config: StageConfig{Name: "Review", MaxDays: 4},
		Queue: []*Application{
			{StageEnteredDay: 10},
			{StageEnteredDay: 8},
		},
		InProgress: []*Application{
			{StageEnteredDay: 5},
		},
	}

	avgAge, p50, p90, oldest, overdue, nearDue := computeStageAging(stage, 10, 1)
	if avgAge < 3.2 || avgAge > 3.4 {
		t.Fatalf("expected avg age near 3.33, got %.2f", avgAge)
	}
	if p50 != 3 {
		t.Fatalf("expected p50 age 3, got %d", p50)
	}
	if p90 != 5 {
		t.Fatalf("expected p90 age 5, got %d", p90)
	}
	if oldest != 6 {
		t.Fatalf("expected oldest age 6, got %d", oldest)
	}
	if overdue != 1 {
		t.Fatalf("expected overdue 1, got %d", overdue)
	}
	if nearDue != 1 {
		t.Fatalf("expected near due 1, got %d", nearDue)
	}
}

func TestStageCycleSummary(t *testing.T) {
	cfg := Config{
		HorizonDays:       2,
		ArrivalRatePerDay: 0,
		ArrivalMode:       "fixed",
		StageNearDueDays:  1,
		Stages: []StageConfig{
			{Name: "Review", CapacityPerDay: 1, MinDays: 1, MaxDays: 3},
		},
	}
	stage := &StageState{
		Config:            cfg.Stages[0],
		QueueSamples:      []int{0, 0},
		ActiveSamples:     []int{0, 0},
		ArrivalDaily:      []int{0, 0},
		CompletedDaily:    []int{1, 1},
		StageCycleSum:     10,
		StageCycleSamples: 3,
		StageCycleTimes:   []int{2, 4, 4},
		StageOnTime:       1,
		StageOverMax:      2,
	}

	report := buildReport(cfg, []*StageState{stage}, []int{}, 0, []*Application{})
	if len(report.StageSummaries) != 1 {
		t.Fatalf("expected 1 stage summary, got %d", len(report.StageSummaries))
	}
	summary := report.StageSummaries[0]
	if summary.StageCycleAvg < 3.32 || summary.StageCycleAvg > 3.34 {
		t.Fatalf("expected stage cycle avg near 3.33, got %.2f", summary.StageCycleAvg)
	}
	if summary.StageCycleP50 != 4 || summary.StageCycleP90 != 4 {
		t.Fatalf("expected stage cycle p50/p90 4, got %d/%d", summary.StageCycleP50, summary.StageCycleP90)
	}
	if summary.StageOnTimeRate < 0.332 || summary.StageOnTimeRate > 0.334 {
		t.Fatalf("expected stage on-time rate near 0.333, got %.3f", summary.StageOnTimeRate)
	}
	if summary.StageOverMax != 2 {
		t.Fatalf("expected stage over max 2, got %d", summary.StageOverMax)
	}
}

func TestStageCycleVariability(t *testing.T) {
	cfg := Config{
		HorizonDays:       3,
		ArrivalRatePerDay: 0,
		ArrivalMode:       "fixed",
		StageNearDueDays:  1,
		Stages: []StageConfig{
			{Name: "Review", CapacityPerDay: 1, MinDays: 1, MaxDays: 3},
		},
	}
	stage := &StageState{
		Config:            cfg.Stages[0],
		QueueSamples:      []int{0, 0, 0},
		ActiveSamples:     []int{0, 0, 0},
		ArrivalDaily:      []int{0, 0, 0},
		CompletedDaily:    []int{0, 0, 0},
		StageCycleSum:     12,
		StageCycleSamples: 3,
		StageCycleTimes:   []int{2, 4, 6},
	}

	report := buildReport(cfg, []*StageState{stage}, []int{}, 0, []*Application{})
	summary := report.StageSummaries[0]
	if summary.StageCycleStdDev < 1.62 || summary.StageCycleStdDev > 1.64 {
		t.Fatalf("expected stage cycle std dev near 1.63, got %.2f", summary.StageCycleStdDev)
	}
	if summary.StageCycleCV < 0.40 || summary.StageCycleCV > 0.42 {
		t.Fatalf("expected stage cycle CV near 0.41, got %.2f", summary.StageCycleCV)
	}
}

func TestServiceTimeVariability(t *testing.T) {
	cfg := Config{
		HorizonDays:       3,
		ArrivalRatePerDay: 0,
		ArrivalMode:       "fixed",
		StageNearDueDays:  1,
		Stages: []StageConfig{
			{Name: "Review", CapacityPerDay: 1, MinDays: 1, MaxDays: 3},
		},
	}
	stage := &StageState{
		Config:         cfg.Stages[0],
		QueueSamples:   []int{0, 0, 0},
		ActiveSamples:  []int{0, 0, 0},
		ArrivalDaily:   []int{0, 0, 0},
		CompletedDaily: []int{0, 0, 0},
		ServiceSum:     12,
		ServiceSamples: 3,
		ServiceTimes:   []int{2, 4, 6},
	}

	report := buildReport(cfg, []*StageState{stage}, []int{}, 0, []*Application{})
	summary := report.StageSummaries[0]
	if summary.ServiceTimeStdDev < 1.62 || summary.ServiceTimeStdDev > 1.64 {
		t.Fatalf("expected service std dev near 1.63, got %.2f", summary.ServiceTimeStdDev)
	}
	if summary.ServiceTimeCV < 0.40 || summary.ServiceTimeCV > 0.42 {
		t.Fatalf("expected service CV near 0.41, got %.2f", summary.ServiceTimeCV)
	}
}

func TestCapacitySlack(t *testing.T) {
	cfg := Config{
		HorizonDays:       2,
		ArrivalRatePerDay: 0,
		ArrivalMode:       "fixed",
		StageNearDueDays:  1,
		Stages: []StageConfig{
			{Name: "Review", CapacityPerDay: 10, MinDays: 1, MaxDays: 2},
		},
	}
	stage := &StageState{
		Config:         cfg.Stages[0],
		ActiveSum:      6,
		QueueSamples:   []int{0, 0},
		ActiveSamples:  []int{3, 3},
		ArrivalDaily:   []int{0, 0},
		CompletedDaily: []int{0, 0},
	}

	report := buildReport(cfg, []*StageState{stage}, []int{}, 0, []*Application{})
	summary := report.StageSummaries[0]
	if summary.CapacitySlack < 6.99 || summary.CapacitySlack > 7.01 {
		t.Fatalf("expected capacity slack near 7, got %.2f", summary.CapacitySlack)
	}
	if summary.CapacitySlackPct < 0.69 || summary.CapacitySlackPct > 0.71 {
		t.Fatalf("expected capacity slack pct near 0.7, got %.3f", summary.CapacitySlackPct)
	}
}
