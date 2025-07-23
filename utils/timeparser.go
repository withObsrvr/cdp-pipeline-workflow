package utils

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// TimeSpecification represents time-based configuration for ledger processing
type TimeSpecification struct {
	StartTimeAgo   string    `yaml:"start_time_ago,omitempty" json:"start_time_ago,omitempty"`
	EndTimeAgo     string    `yaml:"end_time_ago,omitempty" json:"end_time_ago,omitempty"`
	StartTime      time.Time `yaml:"start_time,omitempty" json:"start_time,omitempty"`
	EndTime        time.Time `yaml:"end_time,omitempty" json:"end_time,omitempty"`
	ContinuousMode bool      `yaml:"continuous_mode,omitempty" json:"continuous_mode,omitempty"`
}


// ParseDuration parses various duration formats including custom ones like "1y", "6m"
// Supports: y (years), mo (months), w (weeks), d (days), h (hours), m (minutes), s (seconds)
func ParseDuration(duration string) (time.Duration, error) {
	if duration == "" || duration == "now" {
		return 0, nil
	}

	// Regular expression to match duration patterns
	re := regexp.MustCompile(`^(\d+)([a-zA-Z]+)$`)
	matches := re.FindStringSubmatch(strings.TrimSpace(duration))
	
	if len(matches) != 3 {
		// Try parsing as standard Go duration
		return time.ParseDuration(duration)
	}

	value, err := strconv.Atoi(matches[1])
	if err != nil {
		return 0, fmt.Errorf("invalid duration value: %s", matches[1])
	}

	unit := strings.ToLower(matches[2])
	
	switch unit {
	case "y", "year", "years":
		return time.Duration(value) * 365 * 24 * time.Hour, nil
	case "mo", "month", "months":
		return time.Duration(value) * 30 * 24 * time.Hour, nil
	case "w", "week", "weeks":
		return time.Duration(value) * 7 * 24 * time.Hour, nil
	case "d", "day", "days":
		return time.Duration(value) * 24 * time.Hour, nil
	case "h", "hour", "hours":
		return time.Duration(value) * time.Hour, nil
	case "m", "min", "minute", "minutes":
		return time.Duration(value) * time.Minute, nil
	case "s", "sec", "second", "seconds":
		return time.Duration(value) * time.Second, nil
	default:
		return 0, fmt.Errorf("unknown duration unit: %s", unit)
	}
}

// ParseTimeAgo converts a "time ago" string to an absolute time
// Examples: "1y" -> 1 year ago from now, "6m" -> 6 months ago from now
func ParseTimeAgo(timeAgo string, baseTime time.Time) (time.Time, error) {
	if timeAgo == "" || timeAgo == "now" {
		return baseTime, nil
	}

	duration, err := ParseDuration(timeAgo)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse time ago '%s': %w", timeAgo, err)
	}

	return baseTime.Add(-duration), nil
}

// ParseTimeSpec parses a time specification string which could be:
// - A duration (e.g., "1y", "6m", "30d")
// - An ISO 8601 timestamp (e.g., "2024-01-01T00:00:00Z")
// - A relative phrase (e.g., "1 year ago", "6 months back")
func ParseTimeSpec(spec string, baseTime time.Time) (time.Time, error) {
	if spec == "" || spec == "now" {
		return baseTime, nil
	}

	// Try parsing as ISO 8601 timestamp first
	if t, err := time.Parse(time.RFC3339, spec); err == nil {
		return t, nil
	}

	// Try parsing as a date without time
	if t, err := time.Parse("2006-01-02", spec); err == nil {
		return t, nil
	}

	// Check for relative phrases
	spec = strings.ToLower(strings.TrimSpace(spec))
	if strings.HasSuffix(spec, " ago") || strings.HasSuffix(spec, " back") {
		// Remove suffix and parse the duration part
		spec = strings.TrimSuffix(spec, " ago")
		spec = strings.TrimSuffix(spec, " back")
		spec = strings.TrimSpace(spec)
		
		// Handle phrases like "1 year" -> "1y"
		spec = strings.ReplaceAll(spec, " year", "y")
		spec = strings.ReplaceAll(spec, " month", "mo")
		spec = strings.ReplaceAll(spec, " week", "w")
		spec = strings.ReplaceAll(spec, " day", "d")
		spec = strings.ReplaceAll(spec, " hour", "h")
		spec = strings.ReplaceAll(spec, " minute", "m")
		spec = strings.ReplaceAll(spec, " second", "s")
		spec = strings.ReplaceAll(spec, " ", "")
	}

	// Parse as time ago
	return ParseTimeAgo(spec, baseTime)
}

// ValidateTimeSpecification validates a TimeSpecification for logical consistency
func ValidateTimeSpecification(spec *TimeSpecification) error {
	// Check for conflicting specifications
	hasRelativeTime := spec.StartTimeAgo != "" || spec.EndTimeAgo != ""
	hasAbsoluteTime := !spec.StartTime.IsZero() || !spec.EndTime.IsZero()

	if hasRelativeTime && hasAbsoluteTime {
		return fmt.Errorf("cannot specify both relative (time_ago) and absolute (time) specifications")
	}

	// Validate relative times
	if spec.StartTimeAgo != "" {
		if _, err := ParseDuration(spec.StartTimeAgo); err != nil {
			return fmt.Errorf("invalid start_time_ago: %w", err)
		}
	}

	if spec.EndTimeAgo != "" && spec.EndTimeAgo != "now" {
		if _, err := ParseDuration(spec.EndTimeAgo); err != nil {
			return fmt.Errorf("invalid end_time_ago: %w", err)
		}
	}

	// Validate time ranges
	now := time.Now()
	
	if spec.StartTimeAgo != "" && spec.EndTimeAgo != "" && spec.EndTimeAgo != "now" {
		startTime, _ := ParseTimeAgo(spec.StartTimeAgo, now)
		endTime, _ := ParseTimeAgo(spec.EndTimeAgo, now)
		
		if startTime.After(endTime) {
			return fmt.Errorf("start_time_ago (%s) must be further back than end_time_ago (%s)", 
				spec.StartTimeAgo, spec.EndTimeAgo)
		}
	}

	// Validate absolute times
	if !spec.StartTime.IsZero() && !spec.EndTime.IsZero() {
		if spec.StartTime.After(spec.EndTime) {
			return fmt.Errorf("start_time must be before end_time")
		}
	}

	// Validate continuous mode logic
	if spec.ContinuousMode && spec.EndTimeAgo != "" && spec.EndTimeAgo != "now" {
		return fmt.Errorf("continuous_mode cannot be used with end_time_ago (except 'now')")
	}

	if spec.ContinuousMode && !spec.EndTime.IsZero() && spec.EndTime.Before(now) {
		return fmt.Errorf("continuous_mode cannot be used with past end_time")
	}

	return nil
}

// ResolveTimeSpecification converts a TimeSpecification to absolute start/end times
func ResolveTimeSpecification(spec *TimeSpecification, baseTime time.Time) (startTime, endTime time.Time, err error) {
	// Handle relative time specifications
	if spec.StartTimeAgo != "" {
		startTime, err = ParseTimeAgo(spec.StartTimeAgo, baseTime)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("failed to parse start_time_ago: %w", err)
		}
	} else if !spec.StartTime.IsZero() {
		startTime = spec.StartTime
	}

	if spec.EndTimeAgo != "" {
		if spec.EndTimeAgo == "now" {
			endTime = baseTime
		} else {
			endTime, err = ParseTimeAgo(spec.EndTimeAgo, baseTime)
			if err != nil {
				return time.Time{}, time.Time{}, fmt.Errorf("failed to parse end_time_ago: %w", err)
			}
		}
	} else if !spec.EndTime.IsZero() {
		endTime = spec.EndTime
	} else if spec.ContinuousMode {
		// For continuous mode without explicit end, use far future
		endTime = baseTime.Add(100 * 365 * 24 * time.Hour) // 100 years in the future
	}

	return startTime, endTime, nil
}