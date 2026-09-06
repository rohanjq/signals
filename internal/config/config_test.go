package config

import "testing"

func TestLoadDefaultsWithExplicitDevelopmentAuthOverride(t *testing.T) {
	values := map[string]string{"SIGNALD_INSECURE_NO_AUTH": "true"}
	config, err := load(func(key string) (string, bool) { value, ok := values[key]; return value, ok })
	if err != nil {
		t.Fatal(err)
	}
	want := []int{9, 21, 50, 200}
	if len(config.Periods) != len(want) {
		t.Fatalf("periods = %v", config.Periods)
	}
	for index := range want {
		if config.Periods[index] != want[index] {
			t.Fatalf("periods = %v, want %v", config.Periods, want)
		}
	}
	if len(config.Series) != 4 || config.SeedBars < 200 {
		t.Fatalf("series=%v seed=%d", config.Series, config.SeedBars)
	}
}

func TestLoadRejectsUnsafeOrInsufficientConfiguration(t *testing.T) {
	tests := []map[string]string{
		{},
		{"SIGNALD_INSECURE_NO_AUTH": "true", "SIGNALD_EMA_PERIODS": "9,9"},
		{"SIGNALD_INSECURE_NO_AUTH": "true", "SIGNALD_EMA_PERIODS": "200", "SIGNALD_SEED_BARS": "199"},
		{"SIGNALD_API_TOKEN": "token", "SIGNALD_SERIES": "BTCUSDT"},
		{"SIGNALD_API_TOKEN": "token", "SIGNALD_DATABASE_URL": ""},
	}
	for index, values := range tests {
		if _, err := load(func(key string) (string, bool) { value, ok := values[key]; return value, ok }); err == nil {
			t.Fatalf("case %d: expected error", index)
		}
	}
}
