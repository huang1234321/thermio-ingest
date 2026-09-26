// Command gw-sim 是 thermio 网关模拟器（implementation-plan.md IMPL-6）：
// 场景 profile 驱动的 MQTT 上行/下行模拟，供 ingest 管线（IMPL-5）与链路
// E2E（IMPL-9）联调。上行契约见伞仓 docs/design/ingest.md §2–§3。
//
// 凭证纪律（IMPL-6 验收：走真实认证链路，不做白名单后门）：
//
//	broker 地址   环境变量 GWSIM_BROKER_URL（或 -broker 覆盖）
//	用户名        profile gateways.username_template（{serial}@{tenant}，emqx.md §3.2）
//	密码          环境变量（profile gateways.password_env 指名，默认 GWSIM_MQTT_PASSWORD）
//
// 密码不进 profile、不进命令行、不落日志（SEC-KEY-01/06、CODE-LOG-01）。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

func main() {
	var (
		profilePath = flag.String("profile", "", "场景 profile JSON 文件（必填）")
		broker      = flag.String("broker", "", "MQTT broker URL（默认取 GWSIM_BROKER_URL，再默认 tcp://localhost:1883）")
		duration    = flag.Duration("duration", 0, "运行时长（0 = 用 profile 的 duration_s；都为 0 则直到 SIGINT）")
		seed        = flag.Int64("seed", 0, "随机种子（0 = 用 profile 值）")
		summaryFile = flag.String("summary-file", "", "运行摘要 JSON 输出文件（E2E 留痕用）")
		verbose     = flag.Bool("v", false, "DEBUG 日志")
	)
	flag.Parse()

	if *profilePath == "" {
		fmt.Fprintln(os.Stderr, "usage: gw-sim -profile <path> [-broker url] [-duration 5m] [-seed 1] [-summary-file run.json]")
		os.Exit(2)
	}
	profile, err := LoadProfile(*profilePath)
	if err != nil {
		fatal(err)
	}
	if *seed != 0 {
		profile.Seed = *seed
	}

	brokerURL := strings.TrimSpace(*broker)
	if brokerURL == "" {
		brokerURL = strings.TrimSpace(os.Getenv("GWSIM_BROKER_URL"))
	}
	if brokerURL == "" {
		brokerURL = "tcp://localhost:1883"
	}
	password := os.Getenv(profile.Gateways.PasswordEnv)
	if password == "" {
		fatal(fmt.Errorf("password env %s is empty（SEC-KEY-01：设备密码只经环境变量注入；无匿名/白名单后门）",
			profile.Gateways.PasswordEnv))
	}

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	// 运行生命周期：SIGINT/SIGTERM 或时长到期取消；全部网关退出后兜底取消。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	go func() {
		select {
		case <-sigCh:
			logger.Info("signal received; draining")
			cancel()
		case <-ctx.Done():
		}
	}()
	runFor := *duration
	if runFor == 0 && profile.DurationS > 0 {
		runFor = time.Duration(profile.DurationS) * time.Second
	}
	if runFor > 0 {
		go func() {
			select {
			case <-time.After(runFor):
				logger.Info("duration reached; draining", "duration", runFor.String())
				cancel()
			case <-ctx.Done():
			}
		}()
	}

	stats := newRunStats()
	logger.Info("gw-sim starting",
		"profile", profile.Name, "broker", redactBroker(brokerURL),
		"gateways", profile.Gateways.Count,
		"points", profile.Points.Count+profile.Points.Setpoints,
		"sample_interval_s", profile.SampleIntervalS)

	var wg sync.WaitGroup
	for i := 0; i < profile.Gateways.Count; i++ {
		g := newGatewaySim(profile, i, brokerURL, password, stats, logger)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := g.Run(ctx); err != nil {
				logger.Error("gateway sim exited with error", "gw", g.serial, "err", err.Error())
			}
			g.mergeInjectorStats()
		}()
	}

	// 周期性进度日志（长稳场景的执行留痕）。
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		lastPoints := int64(0)
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				snap := stats.snapshot()
				p := snap["points_published"].(int64)
				logger.Info("progress",
					"points_total", p, "points_per_30s", p-lastPoints,
					"messages", snap["messages_published"],
					"publish_errors", snap["publish_errors"])
				lastPoints = p
			}
		}
	}()

	wg.Wait()
	cancel() // 网关全退（含连接快速失败）也要放行进度/信号协程
	<-progressDone

	stats.finish()
	summary := stats.snapshot()
	summary["profile"] = profile.Name
	summary["gateways"] = profile.Gateways.Count
	logger.Info("gw-sim finished", "summary", summary)

	if *summaryFile != "" {
		if dir := filepath.Dir(*summaryFile); dir != "" && dir != "." {
			_ = os.MkdirAll(dir, 0o750)
		}
		b, err := json.MarshalIndent(summary, "", "  ")
		if err == nil {
			err = os.WriteFile(*summaryFile, b, 0o640)
		}
		if err != nil {
			logger.Error("write summary file failed", "path", *summaryFile, "err", err.Error())
		}
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "gw-sim:", err)
	os.Exit(1)
}
