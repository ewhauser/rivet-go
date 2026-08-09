package rivet

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ewhauser/rivet-go/internal/pump"
	"github.com/fxamacker/cbor/v2"
)

const maxScheduledArgs = 1 << 20

// ActionSchedules exposes RivetKit's durable one-shot action schedules. Each
// schedule has its own stable ID and can be inspected or cancelled without
// affecting the compatibility OnAlarm schedule.
type ActionSchedules struct {
	session *pump.ActorSession
	actions map[string]struct{}
}

// ScheduledAction describes one pending durable action schedule. Arguments is
// the same CBOR argument array that will be delivered through normal action
// dispatch.
type ScheduledAction struct {
	ID        string
	Action    string
	Arguments []byte
	RunAt     time.Time
}

func newActionSchedules[T any](session *pump.ActorSession, actions Actions[T]) *ActionSchedules {
	names := make(map[string]struct{}, len(actions))
	for name := range actions {
		names[name] = struct{}{}
	}
	return &ActionSchedules{session: session, actions: names}
}

// After schedules a registered action relative to RivetKit core's current
// time. Argument is encoded as the action's single typed CBOR argument.
func (s *ActionSchedules) After(
	ctx context.Context,
	delay time.Duration,
	action string,
	argument any,
) (string, error) {
	if delay < 0 {
		return "", errors.New("schedule delay must not be negative")
	}
	args, err := s.encode(ctx, action, argument)
	if err != nil {
		return "", err
	}
	id, err := s.session.ScheduleAfter(ctx, delay, action, args)
	if err != nil {
		return "", fmt.Errorf("schedule action %q after %s: %w", action, delay, err)
	}
	return id, nil
}

// At schedules a registered action at an absolute time.
func (s *ActionSchedules) At(
	ctx context.Context,
	at time.Time,
	action string,
	argument any,
) (string, error) {
	args, err := s.encode(ctx, action, argument)
	if err != nil {
		return "", err
	}
	id, err := s.session.ScheduleAt(ctx, at.UnixMilli(), action, args)
	if err != nil {
		return "", fmt.Errorf("schedule action %q at %s: %w", action, at.Format(time.RFC3339Nano), err)
	}
	return id, nil
}

// Cancel removes one pending schedule. It returns false if that ID is no
// longer pending.
func (s *ActionSchedules) Cancel(ctx context.Context, id string) (bool, error) {
	if err := s.available(ctx); err != nil {
		return false, err
	}
	if id == "" {
		return false, errors.New("schedule ID is empty")
	}
	cancelled, err := s.session.CancelSchedule(ctx, id)
	if err != nil {
		return false, fmt.Errorf("cancel schedule %q: %w", id, err)
	}
	return cancelled, nil
}

// Get returns one pending schedule, or nil if it no longer exists.
func (s *ActionSchedules) Get(ctx context.Context, id string) (*ScheduledAction, error) {
	if err := s.available(ctx); err != nil {
		return nil, err
	}
	if id == "" {
		return nil, errors.New("schedule ID is empty")
	}
	event, found, err := s.session.GetSchedule(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get schedule %q: %w", id, err)
	}
	if !found {
		return nil, nil
	}
	schedule := publicScheduledAction(event)
	return &schedule, nil
}

// List returns every pending action schedule in core's run order. The
// compatibility OnAlarm schedule is intentionally hidden.
func (s *ActionSchedules) List(ctx context.Context) ([]ScheduledAction, error) {
	if err := s.available(ctx); err != nil {
		return nil, err
	}
	events, err := s.session.ListSchedules(ctx)
	if err != nil {
		return nil, fmt.Errorf("list schedules: %w", err)
	}
	schedules := make([]ScheduledAction, len(events))
	for index, event := range events {
		schedules[index] = publicScheduledAction(event)
	}
	return schedules, nil
}

// DecodeArgument decodes this schedule's single typed action argument into
// destination, which must be a non-nil pointer.
func (s ScheduledAction) DecodeArgument(destination any) error {
	if destination == nil {
		return errors.New("schedule argument destination is nil")
	}
	var arguments []cbor.RawMessage
	if err := cbor.Unmarshal(s.Arguments, &arguments); err != nil {
		return fmt.Errorf("decode scheduled argument array: %w", err)
	}
	if len(arguments) != 1 {
		return fmt.Errorf("scheduled action requires one argument, received %d", len(arguments))
	}
	if err := cbor.Unmarshal(arguments[0], destination); err != nil {
		return fmt.Errorf("decode scheduled argument: %w", err)
	}
	return nil
}

func (s *ActionSchedules) encode(ctx context.Context, action string, argument any) ([]byte, error) {
	if err := s.available(ctx); err != nil {
		return nil, err
	}
	if action == "" {
		return nil, errors.New("scheduled action is empty")
	}
	if _, registered := s.actions[action]; !registered {
		return nil, fmt.Errorf("scheduled action %q is not registered", action)
	}
	args, err := cbor.Marshal([]any{argument})
	if err != nil {
		return nil, fmt.Errorf("encode scheduled action %q argument: %w", action, err)
	}
	if len(args) > maxScheduledArgs {
		return nil, fmt.Errorf("scheduled action %q arguments are %d bytes, maximum is %d", action, len(args), maxScheduledArgs)
	}
	return args, nil
}

func (s *ActionSchedules) available(ctx context.Context) error {
	if s == nil {
		return errors.New("actor schedules are unavailable")
	}
	if ctx == nil {
		return errors.New("schedule context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.session == nil {
		return errors.New("actor schedules are unavailable")
	}
	return nil
}

func publicScheduledAction(event pump.ScheduledEvent) ScheduledAction {
	return ScheduledAction{
		ID:        event.ID,
		Action:    event.Action,
		Arguments: append([]byte(nil), event.Args...),
		RunAt:     time.UnixMilli(event.RunAt).UTC(),
	}
}
