package main

import "testing"

func TestLoadConfig(t *testing.T) {
	t.Setenv("APP_HOST", "")
	t.Setenv("APP_PORT", "")
	t.Setenv("GRPC_REFLECTION", "")

	tests := []struct {
		name           string
		env            map[string]string
		wantAddr       string
		wantReflection bool
		wantErr        bool
	}{
		{name: "defaults", env: nil, wantAddr: ":9090", wantReflection: false},
		{name: "different host", env: map[string]string{"APP_HOST": "example.com"}, wantAddr: "example.com:9090"},
		{name: "custom port", env: map[string]string{"APP_PORT": "8080"}, wantAddr: ":8080"},
		{name: "port not a number", env: map[string]string{"APP_PORT": "abc"}, wantErr: true},
		{name: "port too small", env: map[string]string{"APP_PORT": "0"}, wantErr: true},
		{name: "port too big", env: map[string]string{"APP_PORT": "70000"}, wantErr: true},
		{name: "wrong GRPC_REFLECTION", env: map[string]string{"GRPC_REFLECTION": "70000"}, wantErr: true},
		{name: "enable by 1 GRPC_REFLECTION", env: map[string]string{"GRPC_REFLECTION": "1"}, wantReflection: true},
		{name: "enable by true GRPC_REFLECTION", env: map[string]string{"GRPC_REFLECTION": "true"}, wantReflection: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.env != nil {
				for key, value := range tc.env {
					t.Setenv(key, value)
				}
			}

			cfg, err := loadConfig()
			if (err != nil) != tc.wantErr {
				t.Fatalf("loadConfig() error = %v, wantErr %v", err, tc.wantErr)
			}

			if tc.wantErr {
				return
			}

			if tc.wantAddr != "" && cfg.gRPCAddr != tc.wantAddr {
				t.Fatalf("loadConfig() wrog parsed addres got = %v, expected %v", cfg.gRPCAddr, tc.wantAddr)
			}

			if cfg.gRPCReflection != tc.wantReflection {
				t.Fatalf("loadConfig() wrog reflection got = %v, expected %v", cfg.gRPCReflection, tc.wantReflection)
			}
		})
	}
}
