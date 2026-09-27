package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestCommittedProfilesValid 所有随库 profile 必须可加载可校验（README 场景矩阵
// 的每一行对应一个文件——坏文件 = 场景不可复现）。
func TestCommittedProfilesValid(t *testing.T) {
	entries, err := os.ReadDir("profiles")
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		path := filepath.Join("profiles", e.Name())
		p, err := LoadProfile(path)
		if err != nil {
			t.Errorf("%s: %v", path, err)
			continue
		}
		if p.Name == "" || p.Name == "x" {
			t.Errorf("%s: empty name", path)
		}
		// 每份 profile 必须能实例化网关（点位集构建不 panic）。
		g := newGatewaySim(p, 0, "tcp://localhost:1883", "pw", newRunStats(), testLogger)
		if len(g.numPoints)+len(g.spPoints) == 0 {
			t.Errorf("%s: no points built", path)
		}
		n++
	}
	if n < 20 {
		t.Errorf("profiles = %d (README 矩阵预期 ≥ 20)", n)
	}
}

// DAT-165：offset + overrides 旋钮校验。
func TestValidateOffsetAndOverrides(t *testing.T) {
	base := func() Profile {
		return Profile{
			Name: "t", SampleIntervalS: 5, Points: PointsCfg{
				Count: 2, NumericRatio: 1.0, Units: []string{"degC"},
			},
		}
	}
	t.Run("offset-negative-rejected", func(t *testing.T) {
		p := base()
		p.Points.Offset = -1
		if err := p.Validate(); err == nil {
			t.Fatal("want error for negative offset")
		}
	})
	t.Run("override-bad-kind-rejected", func(t *testing.T) {
		p := base()
		p.Points.Overrides = map[string]PointOverride{
			"SIM_0000": {Kind: "weird"},
		}
		if err := p.Validate(); err == nil {
			t.Fatal("want error for bad override kind")
		}
	})
	t.Run("valid-override-accepted", func(t *testing.T) {
		p := base()
		v := 7.0
		p.Points.Offset = 100
		p.Points.Overrides = map[string]PointOverride{
			"SIM_0100": {Base: &v, Unit: "degC"},
		}
		if err := p.Validate(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}
