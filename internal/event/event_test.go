package event

import "testing"

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		e       Event
		wantErr error
	}{
		{"valid", Event{SourceID: "a", EventType: "view"}, nil},
		{"missing source", Event{EventType: "view"}, ErrMissingSourceID},
		{"missing type", Event{SourceID: "a"}, ErrMissingEventType},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.e.Validate(); err != tc.wantErr {
				t.Fatalf("got %v, want %v", err, tc.wantErr)
			}
		})
	}
}
