package pipelines

import (
	"fmt"
	"time"
)

// RemainingTime calculates the duration remaining from now until the given timestamp.
// The timestamp must be in RFC3339 / ISO 8601 format (e.g., "2026-09-03T13:57:17.560506+00:00").
// Returns a negative duration if the timestamp is in the past.
func RemainingTime(timestamp string) (time.Duration, error) {
	target, err := time.Parse(time.RFC3339Nano, timestamp)
	if err != nil {
		return 0, fmt.Errorf("parsing timestamp %q: %w", timestamp, err)
	}
	return time.Until(target), nil
}

// RemainingTimeHuman returns a human-readable string of the remaining time.
func RemainingTimeHuman(timestamp string) (string, error) {
	remaining, err := RemainingTime(timestamp)
	if err != nil {
		return "", err
	}

	if remaining < 0 {
		return fmt.Sprintf("%s ago", formatDuration(-remaining)), nil
	}
	return fmt.Sprintf("%s remaining", formatDuration(remaining)), nil
}

func formatDuration(d time.Duration) string {
	days := int(d.Hours()) / 24
	hours := int(d.Hours()) % 24
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60

	switch {
	case days > 0:
		return fmt.Sprintf("%dd %dh %dm %ds", days, hours, minutes, seconds)
	case hours > 0:
		return fmt.Sprintf("%dh %dm %ds", hours, minutes, seconds)
	case minutes > 0:
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	default:
		return fmt.Sprintf("%ds", seconds)
	}
}

// ShorterEol compares two timestamps and returns the earlier one.
// (i.e., the one with less remaining time)
func ShorterEol(eol1, eol2 string) (string, error) {
	t1, err := time.Parse(time.RFC3339Nano, eol1)
	if err != nil {
		return "", fmt.Errorf("parsing first EOL timestamp %q: %w", eol1, err)
	}

	t2, err := time.Parse(time.RFC3339Nano, eol2)
	if err != nil {
		return "", fmt.Errorf("parsing second EOL timestamp %q: %w", eol2, err)
	}

	if t1.Before(t2) {
		return eol1, nil
	}
	return eol2, nil
}
