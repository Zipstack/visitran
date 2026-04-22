# ADR-001: User-Facing Activity Logs

## Status

Accepted — implemented in PR #66

## Date

2026-04-17

## Context

The bottom Logs panel showed raw developer-internal messages like
`Executing Model Node: Database: globe_testing Database Type: postgres ...`.
Users had no way to tell what happened after triggering an action. We needed
plain-language activity messages ("Model mdoela built successfully in 0.42s")
without breaking the existing developer log pipeline.

### Constraints

- The event pipeline (`fire_event` -> `EventManager` -> `LogHelper` -> Celery -> Socket.IO) is shared across 100+ developer events. Any change must be additive.
- WebSocket rooms are keyed by session ID. We cannot add a second channel without frontend proxy changes (React dev server doesn't proxy WS by default).
- Events are defined as dataclasses backed by protobuf message types. Adding new events is cheap; modifying existing ones risks breaking the proto contract.

## Options Considered

### A. Add an `audience` flag to existing events

Reuse existing event classes. Add `audience="user"` to a subset of them.

- **Pro:** No new classes. Smallest diff.
- **Con:** Developer events have different message formats, verbosity, and lifecycle semantics. Retrofitting "user-friendly" messages onto events designed for `DEBUG`/`INFO` log dumps is fragile. A developer event like `MaterializationTypeTable` fires mid-execution with internal context — rewriting its `message()` to be user-friendly breaks developer debugging.

**Rejected** — mixes concerns, high coupling between developer and user message formats.

### B. Separate `UserLevel` base class (chosen)

New `UserLevel(BaseEvent)` dataclass that overrides `audience()` to return `"user"`. 19 new event classes (U001-U019) in 5 priority tiers. Socket payload gains an `audience` field. Frontend filters by audience in the log-level dropdown.

- **Pro:** Clean separation. Existing events untouched. Adding a new user event = one class + one `fire_event()` call. Rich UI metadata (`title()`, `subtitle()`, `event_status()`) lives on the event class, not in the transport layer.
- **Con:** 19 new classes. More code than option A.

**Accepted** — separation of concerns outweighs class count.

### C. Separate WebSocket channel for user logs

Dedicated socket event (`user_activity`) instead of reusing the `logs:{session_id}` channel.

- **Pro:** Complete decoupling. Frontend subscribes to two events independently.
- **Con:** Duplicates the LogHelper -> Celery -> socket pipeline. Frontend must manage two subscriptions. Mixed "All logs" view requires merging two streams client-side. React dev server proxy needs `ws: true` configuration for each channel.

**Rejected** — too much infrastructure duplication for a filtering problem.

## Decision

**Option B.** Single inheritance (`UserLevel` extends `BaseEvent`) keeps the event pipeline unified while providing a clean audience separation point.

### Pipeline flow

```
fire_event(UserLevelEvent)
  -> EventManager.fire_event()
  -> Logger.write_line()          # checks audience()
     if "user": build rich payload {title, subtitle, status, code, timestamp}
     if "developer": build plain payload {level, message}
  -> LogHelper.publish_log()      # Celery queue
  -> logs_consumer task
  -> sio.emit("logs:{session_id}")
  -> Frontend filters by audience
```

### Key design choices

1. **Audience is a class-level method, not a message field.** `UserLevel.audience()` returns `"user"` — the event *is* user-facing by type, not by configuration. This prevents accidentally marking a developer event as user-facing.

2. **Rich metadata on the event class.** `title()`, `subtitle()`, `event_status()` are methods on `UserLevel`, not fields on the socket payload. The transport layer reads them; the event class owns the rendering logic.

3. **Frontend "User activity" as default view.** The dropdown defaults to showing only `audience === "user"` entries. Developer logs are one click away ("All logs") but not the default noise.

4. **Events fire after success, not inside try.** Each `fire_event()` call is placed after the operation succeeds to prevent false activity entries. If the event itself fails, the existing `write_line` try/except logs it without interrupting the operation.

## Consequences

- Every new user-facing action requires a new `UserLevel` subclass + proto type + `fire_event()` call at the correct point in the operation.
- The `audience` field is now part of the WebSocket payload contract — removing it would break frontend filtering.
- Developer logs are completely unaffected — zero changes to existing ~100 event classes.
- The 5-tier priority structure (P1 core ops, P2 scheduler, P3 CRUD, P4 connections, P5 environments) provides a framework for deciding which future actions deserve user-facing events.

## References

- PR #66: feat: user-facing activity logs with 19 curated events
- PR #59: log-level filter infrastructure (prerequisite)
- Event definitions: `backend/visitran/events/types.py` (lines 829-1153)
- UserLevel base class: `backend/visitran/events/base_types.py` (lines 94-114)
- Audience routing: `backend/visitran/events/eventmgr.py` (lines 126-151)
