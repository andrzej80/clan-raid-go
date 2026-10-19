package config

import (
	"slices"
	"testing"
)

func TestPublicServices(t *testing.T) {
	t.Parallel()

	const (
		health    = "grpc.health.v1.Health"
		reflect   = "grpc.reflection.v1.ServerReflection"
		reflectV1 = "grpc.reflection.v1alpha.ServerReflection"
		raid      = "raidboss.v1.RaidService"
	)

	off := (&Config{GRPCReflection: false}).PublicServices()
	if !slices.Contains(off, health) || slices.Contains(off, reflect) || slices.Contains(off, reflectV1) {
		t.Fatalf("reflection off: got %v, want only health", off)
	}

	on := (&Config{GRPCReflection: true}).PublicServices()
	for _, want := range []string{health, reflect, reflectV1} {
		if !slices.Contains(on, want) {
			t.Fatalf("reflection on: %v must contain %s", on, want)
		}
	}
	if slices.Contains(on, raid) {
		t.Fatalf("business service must never be public: %v", on)
	}
}
