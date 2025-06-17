package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"
)

type Config struct {
	HorizonDays       int           `json:"horizon_days"`
	ArrivalRatePerDay int           `json:"arrival_rate_per_day"`
	TargetCycleDays   int           `json:"target_cycle_days"`
	NearDueWindowDays int           `json:"near_due_window_days"`
	Stages            []StageConfig `json:"stages"`
}

type StageConfig struct {
	Name           string `json:"name"`
	CapacityPerDay int    `json:"capacity_per_day"`
	MinDays        int    `json:"min_days"`
	MaxDays        int    `json:"max_days"`
}

type Application struct {
	ID              int
	ArrivalDay      int
	StageIndex      int
	StageEnteredDay int
	Remaining       int
	CompletedDay    int
}

type StageState struct {
	Config        StageConfig
	Queue         []*Application
	InProgress    []*Application
	QueueSum      int
	ActiveSum     int
	CompletedSum  int
	QueueDays     int
	ActiveDays    int
	QueueSamples  []int
	ActiveSamples []int
}

type Report struct {
	HorizonDays       int               `json:"horizon_days"`
	ArrivalRatePerDay int               `json:"arrival_rate_per_day"`
	TotalArrivals     int               `json:"total_arrivals"`
	TotalCompleted    int               `json:"total_completed"`
	CompletionRate    float64           `json:"completion_rate"`
	AverageCycleDays  float64           `json:"average_cycle_days"`
	Percentiles       map[string]int    `json:"percentiles"`
	WIPTotal          int               `json:"wip_total"`
	StageSummaries    []StageSummary    `json:"stage_summaries"`
	BacklogHighlights BacklogHighlights `json:"backlog_highlights"`
	RiskSummary       RiskSummary       `json:"risk_summary"`
	ConstraintSummary ConstraintSummary `json:"constraint_summary"`
}

type StageSummary struct {
	Name              string  `json:"name"`
	Capacity          int     `json:"capacity_per_day"`
	MaxDays           int     `json:"max_days"`
	AverageQueue      float64 `json:"average_queue"`
	AverageActive     float64 `json:"average_active"`
	Utilization       float64 `json:"utilization_rate"`
	EstimatedWaitDays float64 `json:"estimated_wait_days"`
	Pressure          string  `json:"pressure"`
	WIP               int     `json:"wip"`
	AverageAgeDays    float64 `json:"average_age_days"`
	OldestAgeDays     int     `json:"oldest_age_days"`
	OverdueWIP        int     `json:"overdue_wip"`
	NearDueWIP        int     `json:"near_due_wip"`
	QueueDaysPct      float64 `json:"queue_days_pct"`
	ActiveDaysPct     float64 `json:"active_days_pct"`
	QueueVolatility   float64 `json:"queue_volatility"`
	FlowEfficiency    float64 `json:"flow_efficiency"`
	ThroughputPerDay  float64 `json:"throughput_per_day"`
}

type BacklogHighlights struct {
	TopAverageQueue string `json:"top_average_queue"`
	TopWIP          string `json:"top_wip"`
}

type RiskSummary struct {
	TargetCycleDays   int     `json:"target_cycle_days"`
	NearDueWindowDays int     `json:"near_due_window_days"`
	OnTimeRate        float64 `json:"on_time_rate"`
	OverdueCompleted  int     `json:"overdue_completed"`
	OverdueWIP        int     `json:"overdue_wip"`
	NearDueWIP        int     `json:"near_due_wip"`
}

type ConstraintSummary struct {
	Stage             string  `json:"stage"`
	ArrivalRatePerDay int     `json:"arrival_rate_per_day"`
	ThroughputPerDay  float64 `json:"throughput_per_day"`
	ThroughputGap     float64 `json:"throughput_gap"`
	Utilization       float64 `json:"utilization_rate"`
	AverageQueue      float64 `json:"average_queue"`
	Recommendation    string  `json:"recommendation"`
}

const sampleConfig = `{
  "horizon_days": 60,
  "arrival_rate_per_day": 18,
  "target_cycle_days": 21,
  "near_due_window_days": 3,
  "stages": [
    {"name": "Intake", "capacity_per_day": 20, "min_days": 1, "max_days": 2},
    {"name": "Eligibility Review", "capacity_per_day": 14, "min_days": 2, "max_days": 5},
    {"name": "Committee", "capacity_per_day": 10, "min_days": 3, "max_days": 6},
    {"name": "Final Decision", "capacity_per_day": 12, "min_days": 1, "max_days": 3}
  ]
}
`

func main() {
	configPath := flag.String("config", "", "Path to JSON config")
	seed := flag.Int64("seed", time.Now().UnixNano(), "Random seed")
	format := flag.String("format", "text", "Output format: text or json")
	writeSample := flag.String("write-sample", "", "Write sample config to path and exit")
	flag.Parse()

	if *writeSample != "" {
		if err := os.WriteFile(*writeSample, []byte(sampleConfig), 0o644); err != nil {
			fatal(err)
		}
		fmt.Printf("Wrote sample config to %s\n", *writeSample)
		return
	}

	cfg, err := loadConfig(*configPath)
	if err != nil {
		fatal(err)
	}

	if err := validateConfig(cfg); err != nil {
		fatal(err)
	}

	rng := rand.New(rand.NewSource(*seed))
	report := simulate(cfg, rng)

	if strings.EqualFold(*format, "json") {
		payload, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			fatal(err)
		}
		fmt.Println(string(payload))
		return
	}

	printReport(report)
}

func loadConfig(path string) (Config, error) {
	if path == "" {
		var cfg Config
		if err := json.Unmarshal([]byte(sampleConfig), &cfg); err != nil {
			return Config{}, err
		}
		applyDefaults(&cfg)
		return cfg, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, err
	}
	applyDefaults(&cfg)
	return cfg, nil
}

func applyDefaults(cfg *Config) {
	if cfg.NearDueWindowDays == 0 {
		cfg.NearDueWindowDays = 3
	}
}

func validateConfig(cfg Config) error {
	if cfg.HorizonDays <= 0 {
		return errors.New("horizon_days must be > 0")
	}
	if cfg.ArrivalRatePerDay < 0 {
		return errors.New("arrival_rate_per_day must be >= 0")
	}
	if cfg.TargetCycleDays < 0 {
		return errors.New("target_cycle_days must be >= 0")
	}
	if cfg.NearDueWindowDays < 0 {
		return errors.New("near_due_window_days must be >= 0")
	}
	if len(cfg.Stages) == 0 {
		return errors.New("stages must include at least one stage")
	}
	for _, stage := range cfg.Stages {
		if stage.Name == "" {
			return errors.New("stage name is required")
		}
		if stage.CapacityPerDay < 0 {
			return fmt.Errorf("stage %s capacity_per_day must be >= 0", stage.Name)
		}
		if stage.MinDays <= 0 || stage.MaxDays <= 0 {
			return fmt.Errorf("stage %s min_days and max_days must be > 0", stage.Name)
		}
		if stage.MinDays > stage.MaxDays {
			return fmt.Errorf("stage %s min_days must be <= max_days", stage.Name)
		}
	}
	return nil
}

func simulate(cfg Config, rng *rand.Rand) Report {
	stages := make([]*StageState, len(cfg.Stages))
	for i, stage := range cfg.Stages {
		stages[i] = &StageState{Config: stage}
	}

	var applications []*Application
	var completedDurations []int
	idCounter := 0

	for day := 1; day <= cfg.HorizonDays; day++ {
		for i := 0; i < cfg.ArrivalRatePerDay; i++ {
			idCounter++
			app := &Application{ID: idCounter, ArrivalDay: day, StageIndex: 0, StageEnteredDay: day}
			applications = append(applications, app)
			stages[0].Queue = append(stages[0].Queue, app)
		}

		for idx, stage := range stages {
			var stillWorking []*Application
			for _, app := range stage.InProgress {
				app.Remaining--
				if app.Remaining > 0 {
					stillWorking = append(stillWorking, app)
					continue
				}

				stage.CompletedSum++
				if idx == len(stages)-1 {
					cycle := day - app.ArrivalDay + 1
					app.CompletedDay = day
					completedDurations = append(completedDurations, cycle)
				} else {
					app.StageIndex = idx + 1
					app.StageEnteredDay = day
					stages[idx+1].Queue = append(stages[idx+1].Queue, app)
				}
			}
			stage.InProgress = stillWorking

			capacity := stage.Config.CapacityPerDay - len(stage.InProgress)
			if capacity > 0 && len(stage.Queue) > 0 {
				if capacity > len(stage.Queue) {
					capacity = len(stage.Queue)
				}
				for i := 0; i < capacity; i++ {
					app := stage.Queue[0]
					stage.Queue = stage.Queue[1:]
					app.Remaining = rng.Intn(stage.Config.MaxDays-stage.Config.MinDays+1) + stage.Config.MinDays
					stage.InProgress = append(stage.InProgress, app)
				}
			}

			stage.QueueSum += len(stage.Queue)
			stage.ActiveSum += len(stage.InProgress)
			stage.QueueSamples = append(stage.QueueSamples, len(stage.Queue))
			stage.ActiveSamples = append(stage.ActiveSamples, len(stage.InProgress))
			if len(stage.Queue) > 0 {
				stage.QueueDays++
			}
			if len(stage.InProgress) > 0 {
				stage.ActiveDays++
			}
		}
	}

	return buildReport(cfg, stages, completedDurations, idCounter, applications)
}

func buildReport(cfg Config, stages []*StageState, completed []int, totalArrivals int, apps []*Application) Report {
	completionRate := 0.0
	avgCycle := 0.0
	if totalArrivals > 0 {
		completionRate = float64(len(completed)) / float64(totalArrivals)
	}
	if len(completed) > 0 {
		sum := 0
		for _, d := range completed {
			sum += d
		}
		avgCycle = float64(sum) / float64(len(completed))
	}

	percentiles := computePercentiles(completed, []int{50, 90, 95})

	stageSummaries := make([]StageSummary, 0, len(stages))
	wipTotal := 0
	var topQueue StageSummary
	var topWIP StageSummary
	for _, stage := range stages {
		avgQueue := float64(stage.QueueSum) / float64(cfg.HorizonDays)
		avgActive := float64(stage.ActiveSum) / float64(cfg.HorizonDays)
		utilization := 0.0
		if stage.Config.CapacityPerDay > 0 {
			utilization = avgActive / float64(stage.Config.CapacityPerDay)
		}
		estimatedWait := 0.0
		if stage.Config.CapacityPerDay > 0 {
			estimatedWait = avgQueue / float64(stage.Config.CapacityPerDay)
		}
		wip := len(stage.Queue) + len(stage.InProgress)
		wipTotal += wip
		avgAge, oldestAge, overdueWIP, nearDueWIP := computeStageAging(stage, cfg.HorizonDays)
		flowEfficiency := 0.0
		if avgQueue+avgActive > 0 {
			flowEfficiency = avgActive / (avgQueue + avgActive)
		}
		summary := StageSummary{
			Name:              stage.Config.Name,
			Capacity:          stage.Config.CapacityPerDay,
			MaxDays:           stage.Config.MaxDays,
			AverageQueue:      round(avgQueue, 2),
			AverageActive:     round(avgActive, 2),
			Utilization:       round(utilization, 2),
			EstimatedWaitDays: round(estimatedWait, 2),
			Pressure:          classifyPressure(utilization, estimatedWait),
			WIP:               wip,
			AverageAgeDays:    round(avgAge, 2),
			OldestAgeDays:     oldestAge,
			OverdueWIP:        overdueWIP,
			NearDueWIP:        nearDueWIP,
			QueueDaysPct:      round(float64(stage.QueueDays)/float64(cfg.HorizonDays), 3),
			ActiveDaysPct:     round(float64(stage.ActiveDays)/float64(cfg.HorizonDays), 3),
			QueueVolatility:   round(stdDev(stage.QueueSamples), 2),
			FlowEfficiency:    round(flowEfficiency, 3),
			ThroughputPerDay:  round(float64(stage.CompletedSum)/float64(cfg.HorizonDays), 2),
		}
		stageSummaries = append(stageSummaries, summary)
		if summary.AverageQueue > topQueue.AverageQueue {
			topQueue = summary
		}
		if summary.WIP > topWIP.WIP {
			topWIP = summary
		}
	}

	riskSummary := buildRiskSummary(cfg, completed, apps)
	constraintSummary := buildConstraintSummary(cfg, stageSummaries)

	return Report{
		HorizonDays:       cfg.HorizonDays,
		ArrivalRatePerDay: cfg.ArrivalRatePerDay,
		TotalArrivals:     totalArrivals,
		TotalCompleted:    len(completed),
		CompletionRate:    round(completionRate, 3),
		AverageCycleDays:  round(avgCycle, 2),
		Percentiles:       percentiles,
		WIPTotal:          wipTotal,
		StageSummaries:    stageSummaries,
		BacklogHighlights: BacklogHighlights{TopAverageQueue: topQueue.Name, TopWIP: topWIP.Name},
		RiskSummary:       riskSummary,
		ConstraintSummary: constraintSummary,
	}
}

func buildRiskSummary(cfg Config, completed []int, apps []*Application) RiskSummary {
	risk := RiskSummary{
		TargetCycleDays:   cfg.TargetCycleDays,
		NearDueWindowDays: cfg.NearDueWindowDays,
	}
	if cfg.TargetCycleDays == 0 {
		return risk
	}

	onTime := 0
	for _, d := range completed {
		if d <= cfg.TargetCycleDays {
			onTime++
		}
	}
	if len(completed) > 0 {
		risk.OnTimeRate = round(float64(onTime)/float64(len(completed)), 3)
	}
	risk.OverdueCompleted = len(completed) - onTime

	for _, app := range apps {
		if app.CompletedDay > 0 {
			continue
		}
		age := cfg.HorizonDays - app.ArrivalDay + 1
		if age > cfg.TargetCycleDays {
			risk.OverdueWIP++
			continue
		}
		if cfg.TargetCycleDays-age <= cfg.NearDueWindowDays {
			risk.NearDueWIP++
		}
	}

	return risk
}

func buildConstraintSummary(cfg Config, stages []StageSummary) ConstraintSummary {
	summary := ConstraintSummary{ArrivalRatePerDay: cfg.ArrivalRatePerDay}
	if len(stages) == 0 {
		return summary
	}

	bestGap := -1e9
	var gapStage StageSummary
	bestUtil := -1.0
	var utilStage StageSummary
	for _, stage := range stages {
		gap := float64(cfg.ArrivalRatePerDay) - stage.ThroughputPerDay
		if gap > bestGap {
			bestGap = gap
			gapStage = stage
		}
		if stage.Utilization > bestUtil {
			bestUtil = stage.Utilization
			utilStage = stage
		}
	}

	chosen := utilStage
	if bestGap > 0.1 {
		chosen = gapStage
	}

	gap := float64(cfg.ArrivalRatePerDay) - chosen.ThroughputPerDay
	recommendation := "Maintain current capacity; monitor volatility."
	if gap > 0.1 {
		recommendation = "Increase capacity or reduce service time at this stage."
	}

	return ConstraintSummary{
		Stage:             chosen.Name,
		ArrivalRatePerDay: cfg.ArrivalRatePerDay,
		ThroughputPerDay:  round(chosen.ThroughputPerDay, 2),
		ThroughputGap:     round(gap, 2),
		Utilization:       round(chosen.Utilization, 2),
		AverageQueue:      round(chosen.AverageQueue, 2),
		Recommendation:    recommendation,
	}
}

func computeStageAging(stage *StageState, horizonDays int) (float64, int, int, int) {
	totalAge := 0
	count := 0
	oldest := 0
	overdue := 0
	nearDue := 0
	nearWindow := 1
	apps := append([]*Application{}, stage.Queue...)
	apps = append(apps, stage.InProgress...)
	for _, app := range apps {
		age := horizonDays - app.StageEnteredDay + 1
		if age < 0 {
			continue
		}
		totalAge += age
		count++
		if age > oldest {
			oldest = age
		}
		if stage.Config.MaxDays > 0 {
			if age > stage.Config.MaxDays {
				overdue++
				continue
			}
			if stage.Config.MaxDays-age <= nearWindow {
				nearDue++
			}
		}
	}
	if count == 0 {
		return 0, oldest, overdue, nearDue
	}
	return float64(totalAge) / float64(count), oldest, overdue, nearDue
}

func computePercentiles(values []int, percentiles []int) map[string]int {
	result := map[string]int{}
	if len(values) == 0 {
		for _, p := range percentiles {
			result[fmt.Sprintf("p%d", p)] = 0
		}
		return result
	}

	sorted := append([]int{}, values...)
	sort.Ints(sorted)

	for _, p := range percentiles {
		pos := float64(p) / 100 * float64(len(sorted)-1)
		lower := int(math.Floor(pos))
		upper := int(math.Ceil(pos))
		if lower == upper {
			result[fmt.Sprintf("p%d", p)] = sorted[lower]
			continue
		}
		weight := pos - float64(lower)
		value := float64(sorted[lower])*(1-weight) + float64(sorted[upper])*weight
		result[fmt.Sprintf("p%d", p)] = int(math.Round(value))
	}

	return result
}

func round(value float64, precision int) float64 {
	factor := math.Pow(10, float64(precision))
	return math.Round(value*factor) / factor
}

func classifyPressure(utilization float64, estimatedWait float64) string {
	switch {
	case utilization >= 0.9 || estimatedWait >= 2:
		return "High"
	case utilization >= 0.75 || estimatedWait >= 1:
		return "Medium"
	default:
		return "Low"
	}
}

func printReport(report Report) {
	fmt.Println("Group Scholar Review Latency Lab")
	fmt.Println("--------------------------------")
	fmt.Printf("Horizon: %d days\n", report.HorizonDays)
	fmt.Printf("Arrivals per day: %d\n", report.ArrivalRatePerDay)
	fmt.Printf("Total arrivals: %d\n", report.TotalArrivals)
	fmt.Printf("Completed: %d (%.1f%%)\n", report.TotalCompleted, report.CompletionRate*100)
	fmt.Printf("Average cycle time: %.2f days\n", report.AverageCycleDays)
	fmt.Printf("Cycle time percentiles: p50=%d p90=%d p95=%d\n",
		report.Percentiles["p50"], report.Percentiles["p90"], report.Percentiles["p95"])
	fmt.Printf("Work-in-progress at horizon end: %d\n", report.WIPTotal)
	if report.RiskSummary.TargetCycleDays > 0 {
		fmt.Printf("SLA target: %d days | on-time %.1f%% | overdue completed %d\n",
			report.RiskSummary.TargetCycleDays, report.RiskSummary.OnTimeRate*100, report.RiskSummary.OverdueCompleted)
		fmt.Printf("WIP risk: %d overdue, %d near due (<=%d days to target)\n",
			report.RiskSummary.OverdueWIP, report.RiskSummary.NearDueWIP, report.RiskSummary.NearDueWindowDays)
	}
	if report.ConstraintSummary.Stage != "" {
		fmt.Printf("Constraint focus: %s | gap %.2f/day | util %.2f | avg queue %.2f\n",
			report.ConstraintSummary.Stage,
			report.ConstraintSummary.ThroughputGap,
			report.ConstraintSummary.Utilization,
			report.ConstraintSummary.AverageQueue)
		fmt.Printf("Recommendation: %s\n", report.ConstraintSummary.Recommendation)
	}
	fmt.Println()
	fmt.Println("Stage detail")
	for _, stage := range report.StageSummaries {
		line := fmt.Sprintf("- %s | cap %d/day | avg queue %.2f | avg active %.2f | util %.2f | est wait %.2f days | pressure %s | wip %d | avg age %.2f days | oldest %d days | stage max %d days",
			stage.Name,
			stage.Capacity,
			stage.AverageQueue,
			stage.AverageActive,
			stage.Utilization,
			stage.EstimatedWaitDays,
			stage.Pressure,
			stage.WIP,
			stage.AverageAgeDays,
			stage.OldestAgeDays,
			stage.MaxDays,
		)
		line = fmt.Sprintf("%s | overdue %d | near due %d", line, stage.OverdueWIP, stage.NearDueWIP)
		fmt.Println(line)
	}
	fmt.Println()
	fmt.Printf("Backlog highlight: avg queue leader = %s, top WIP = %s\n",
		report.BacklogHighlights.TopAverageQueue, report.BacklogHighlights.TopWIP)
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "error: %s\n", err)
	os.Exit(1)
}
