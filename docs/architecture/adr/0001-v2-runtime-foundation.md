# ADR-0001: CPAMP 2.0 Runtime Foundation

- Status: Accepted
- Date: 2026-09-14
- Scope: Phase 1 Runtime Foundation

## Context

CPAMP 2.0 separates public transport, product control, privileged runtime execution, and model traffic into distinct ownership domains. The goal is to let the gateway keep serving model traffic when the control plane is unavailable, while keeping privileged process and update operations out of the Manager process.

This ADR freezes the minimum architecture needed to begin implementation. It intentionally does not freeze later Gateway Governance, Public API v2, migration UI, or provider-routing policy.

## Decision

### 1. Product and runtime roles

CPAMP uses three runtime authority roles plus one stateless public transport role:

- **Ingress** — Public transport mux. Owns no product desired state, lifecycle authority, credential policy, or business storage.
- **Manager** — Control Plane. Owns desired product/runtime configuration, user/admin state, usage/analytics, update recommendation policy, and encrypted product secrets.
- **Runtime Supervisor** — Execution Plane for Embedded mode. Owns privileged lifecycle execution and its private operation journal. It does not own product configuration.
- **CPA** — Data Plane. Owns gateway request handling, provider/credential runtime behavior, and model traffic.

The default Embedded product surface exposes one CPAMP endpoint on host port `18317`. Manager `18317`, CPA/Gateway `8317`, and Runtime Protocol `9081` remain internal listeners. Model traffic MUST follow:

```text
AI Client -> CPAMP Ingress -> CPA/Gateway -> Provider
```

Model traffic MUST NOT pass through Manager or Runtime Supervisor.

Manager MAY communicate directly with the CPA Management API for management-domain operations. Runtime Supervisor MUST NOT become a proxy between Manager and the CPA Management API.

### 2. Runtime modes

CPAMP exposes two durable runtime modes:

- `embedded` — CPAMP manages CPA lifecycle through Runtime Supervisor. This is the default/recommended mode.
- `external` — CPAMP connects to a separately managed CPA. This is Advanced Compatibility Mode.

Embedded and External MUST use one application-level `RuntimeClient` contract. Capability negotiation determines which lifecycle/update actions are available. Business code MUST NOT spread mode-specific `if embedded / if external` branches when a capability can express the difference.

Adopting an existing CPA into CPAMP management is a migration path from External to Embedded, not a third runtime mode.

### 3. State ownership

Ownership is fixed as follows:

| State / Action | Authority |
|---|---|
| Runtime mode | Manager |
| Desired CPA connection | Manager |
| CPA Management Key | Manager encrypted storage |
| Desired/recommended version | Manager update policy |
| Product migration checkpoint | Manager |
| Runtime capabilities | Runtime handshake; Manager may cache/project |
| CPA PID / running version / process health | Supervisor / CPA observed state |
| Lifecycle/update operation execution | Supervisor |
| Operation journal / rollback execution state | Supervisor private journal |
| Usage / analytics | Manager |
| CPA gateway/provider runtime | CPA |
| Public request transport and route mux | Ingress, without persisted product state |

Invariant:

> Manager owns desired state; Supervisor owns privileged execution journal; CPA/Supervisor expose observed state; Manager reconciles desired and observed state.

Runtime Supervisor MUST NOT read, open, migrate, or depend on Manager SQLite databases or `data.key`.

### 4. Runtime Protocol v1

Phase 1 uses a versioned **HTTP/JSON** Runtime Protocol.

Transport placement:

- Docker Embedded: private Docker network only; no host publication by default.
- Native Linux/macOS/Windows: loopback by default.

The protocol MUST be authenticated per installation and MUST support explicit timeouts. The implemented surface contains handshake, protocol version, runtime identity/generation, capabilities, status, running CPA version, and typed Start, Stop, and Restart mutations. Additional lifecycle mutations are separate later slices.

#### Runtime generation

Under the mutation-capable Runtime Protocol contract, `RuntimeGeneration` is the **Supervisor execution authority epoch** for one Runtime Supervisor process incarnation. Runtime Supervisor owns the current generation. Manager only observes it through handshake/status, may cache it, and echoes it as a mutation precondition. Manager MUST NOT create, increment, or persist a generation as authority.

The current read-only protocol slice MAY continue to obtain `RuntimeGeneration` from `CPAMP_RUNTIME_GENERATION` as bootstrap metadata. That configured value does not satisfy or enable mutation fencing, and no mutation endpoint may rely on it as execution authority.

Before the first mutation endpoint is enabled, Supervisor startup MUST replace that bootstrap mechanism. A mutation-capable Supervisor MUST establish a new execution authority epoch and freshly sample a cryptographically random, non-zero, opaque `uint64` generation for each process incarnation before serving handshake/status or accepting mutations. Phase 1 treats accidental numeric collision as negligible and does not introduce a durable generation registry or counter solely to prove uniqueness; an implementation MUST NOT intentionally reuse a known generation. Generation is compared for equality only; it has no ordering, monotonic-counter, business-version, desired-state-revision, database-generation, CPA PID, CPA-version, or CPA-restart-count semantics.

Once mutation capability is enabled, CPA stop, start, restart, crash recovery, or binary replacement does not by itself change generation while the same Supervisor process remains the execution authority. After Supervisor restart, Manager MUST observe handshake/status again before submitting another mutation instead of intentionally reusing its cached pre-restart generation. Under Phase 1's random-epoch model, the freshly sampled value makes that cached value stale except for the accepted negligible collision probability.

#### Mutation operation envelope and fencing

Every mutation request MUST combine common mutation metadata with a typed operation request. The common metadata is:

- `operationId`: created by Manager, opaque to Supervisor, non-empty, stable across retries of the same logical mutation, and no more than 128 UTF-8 bytes. The protocol does not require a UUID format.
- `expectedRuntimeIdentity`: the Runtime identity most recently observed by Manager.
- `expectedRuntimeGeneration`: the Runtime generation most recently observed by Manager.

Mutation requests MUST use operation-specific typed payloads. An open-ended `action` plus arbitrary `params` map is not part of Runtime Protocol v1.

Supervisor's durable idempotency namespace is `(RuntimeIdentity, operationId)`. If a private journal is permanently scoped to one immutable Runtime identity, `operationId` alone may be its physical key, but the protocol semantics are the same. `RuntimeGeneration` records the execution authority epoch in which an operation was created; it MUST NOT partition or reset the durable idempotency namespace.

For idempotency comparison, the logical request consists of the operation type and its typed payload. `expectedRuntimeIdentity` selects the Runtime namespace, while `expectedRuntimeGeneration` is a freshness precondition for the current submission; changing only that expected generation after re-observation does not make the logical request different.

Across all retained generations for the same Runtime identity:

- A previously unseen operation ID with valid current Runtime identity/generation is durably recorded before any side effect.
- Replaying an existing operation ID with the same logical typed request returns its existing operation/result state, including the generation in which it was created, and MUST NOT execute the side effect twice.
- Reusing an existing operation ID for a different operation type or typed payload fails with `operation_id_conflict`.

For the lifetime of a Runtime identity, an operation ID is not reusable. Retention or compaction MUST NOT make a recorded operation ID appear previously unseen. Detailed result data may be discarded only if a durable tombstone preserves enough request-identity and outcome metadata to maintain replay behavior, idempotency, and conflict detection.

Mutation submission order is fixed:

1. Authenticate.
2. Decode and perform basic request validation.
3. Compare `expectedRuntimeIdentity` with the current identity.
4. Compare `expectedRuntimeGeneration` with the current generation.
5. Resolve operation ID against the durable cross-generation idempotency namespace.
6. Evaluate operation-specific preconditions for a new operation.
7. Durably record intent for a new operation.
8. Perform the side effect.

An identity mismatch fails with `runtime_identity_mismatch`; a generation mismatch fails with `stale_runtime_generation`. Both checks happen before idempotency lookup, durable intent, and side effects. Consequently, replaying a request that still carries a previous generation is rejected as stale. After Manager observes the current generation and retries the same logical request with the same operation ID, the durable lookup finds the earlier-generation record: an identical request returns the existing state, while a different request fails with `operation_id_conflict`. A generation change alone MUST NOT create or execute the operation again. A future, separate operation-observation API is responsible for querying operation status without resubmitting a mutation.

Runtime Protocol v1 keeps the existing JSON error envelope with a stable code and a human-readable message. The code is the protocol contract; clients MUST NOT branch on or otherwise depend on message wording. Mutation application errors and their HTTP status are:

| HTTP status | Error code | Meaning |
|---|---|---|
| `400` | `invalid_request` | Malformed or invalid mutation request. |
| `400` | `unsupported_operation` | The typed operation is not supported by this Runtime. |
| `409` | `runtime_identity_mismatch` | The request targets a different Runtime identity. |
| `409` | `stale_runtime_generation` | The request targets a different Supervisor authority epoch. |
| `409` | `operation_id_conflict` | The operation ID already names a different logical request for this Runtime identity. |
| `409` | `operation_state_conflict` | The typed operation is incompatible with the current operation/runtime state. |
| `503` | `operation_persistence_unavailable` | Durable operation state cannot be committed, so no side effect is performed. |
| `500` | `internal_error` | An unexpected internal failure occurred. |

HTTP authentication failure remains the existing `401` protocol behavior and does not introduce a parallel domain authentication error system. Future typed operations may define additional stable application codes without changing this common envelope.

A mutation result contains at least `operationId`, `operationType`, `runtimeIdentity`, `runtimeGeneration`, `state`, and an optional structured reason/error with a stable code and a human-readable message. Message text is not a stable API field. The initial state vocabulary is `accepted`, `running`, `succeeded`, and `failed`. This contract does not introduce progress percentages, streaming, queues, or a workflow engine.

#### Typed Start

`POST /v1/runtime/operations/start` accepts only the common mutation envelope:

```json
{
  "operationId": "opaque-id",
  "expectedRuntimeIdentity": "runtime-id",
  "expectedRuntimeGeneration": 123
}
```

The endpoint fixes the operation type to `start`; its typed payload is empty. The request body is limited to 16 KiB and MUST reject unknown or duplicate fields and trailing JSON values. HTTP callers MUST NOT supply executable paths, arguments, environment, working directories, shell commands, or generic action parameters. Logical request identity is the secret-free, versioned canonical value `runtime.start/v1:{}`; the journal stores its SHA-256 fingerprint and compares the operation type separately.

Start, Stop, and Restart require the Supervisor-local settings `CPAMP_RUNTIME_JOURNAL_PATH` and `CPAMP_CPA_EXECUTABLE`. Both absent preserves read-only operation and returns `400 unsupported_operation` for a valid authenticated lifecycle request; configuring only one fails startup. With both configured, Supervisor opens its private journal once at startup, retains one shared lifecycle executor and child manager, and drains accepted execution before closing the journal exactly once at shutdown. It does not terminate the child to compensate for a request or persistence failure.

At spawn, Supervisor MUST explicitly build the CPA child environment by removing the Supervisor-private namespace `CPAMP_RUNTIME_*` and the exact variable `CPAMP_CPA_EXECUTABLE`. Name matching is case sensitive on Unix and case insensitive on Windows. All other parent entries retain their names, values and order, including proxy, certificate, timezone, locale, XDG and non-private `CPAMP_*` settings. An empty result MUST NOT fall back to unfiltered parent environment inheritance. New Supervisor-private settings must use that namespace or be explicitly added to the filter. The working directory remains inherited; this boundary does not introduce a general environment override API or OS privilege sandbox.

Handshake and status advertise the exact capabilities `start`, `stop`, and `restart`, in that order, only when the shared lifecycle executor is configured. Each capability means its typed submission is supported; it does not promise that current process preconditions permit the operation or that a spawned process is ready. Read-only operation MUST NOT advertise any lifecycle capability.

Start, Stop, and Restart submissions share one serialization boundary. After authentication, strict validation, identity/generation fencing and durable operation-ID lookup, an existing operation is returned with HTTP `200`, its retained state and its creation generation. Replay never resumes or repeats execution, including for `accepted` or `running` evidence. A new Start requires `not_started` or confirmed, reaped `exited` process state; `running` or unknown ownership returns `409 operation_state_conflict` before writing intent.

For a new operation, Supervisor commits accepted intent, commits running evidence, spawns the locally configured executable, and commits a terminal result. Required pre-spawn persistence failure returns `503 operation_persistence_unavailable` and MUST NOT spawn. After durable intent commits, the HTTP caller's cancellation or deadline no longer owns execution or child lifetime. Execution remains synchronous without a queue or recovery worker.

Start `succeeded` means only successful OS spawn and child ownership publication. It does not establish readiness, probe listeners or the CPA Management API, populate observed CPA version, or change RuntimeGeneration. Spawn failure attempts to retain a `failed` result with stable code `process_start_failed` and returns `500 internal_error`. Terminal persistence failure also returns `500 internal_error`; the child may already be running, and retained accepted/running evidence MUST prevent a repeated spawn for the same operation ID. Operation observation and recovery are separate capabilities.

#### Typed Stop

`POST /v1/runtime/operations/stop` accepts only the same common mutation envelope as Start. The endpoint fixes the operation type to `stop`; its typed payload is empty, and its logical request identity is `runtime.stop/v1:{}` with a SHA-256 journal fingerprint. The caller cannot provide a PID, signal, force flag, grace or timeout policy, executable, arguments, environment, working directory, shell command, or generic action parameters.

A new Stop requires a confirmed running child that the current `cpaprocess.Manager` owns. The Supervisor reserves that exact owned child handle during the operation precondition and retains the reservation across durable accepted/running evidence and termination. `not_started`, confirmed `exited`, unknown state, unconfirmed ownership, or another active Stop reservation returns `409 operation_state_conflict` before writing intent. The implementation MUST NOT recover a target from an observed PID, search for an OS process, or redirect an older Stop to a replacement child.

After accepted intent and running evidence are durably committed, Stop uses the Supervisor-owned execution context and a cross-platform hard termination primitive on the reserved child handle. Stop `succeeded` requires the same child's `Wait` path to confirm exit, reap it, and release Manager ownership; a successful termination request alone is insufficient. A natural exit racing the termination request may succeed only when that same reserved child is confirmed reaped. Termination failure without confirmed exit attempts to retain `failed` with stable code `process_stop_failed`.

Retained `accepted`, `running`, `succeeded`, or `failed` Stop evidence is replayed without resuming or repeating termination. If the child is already confirmed stopped but terminal persistence fails, the side effect is not rolled back and the retained operation ID MUST NOT terminate a current or later replacement child. The `stop` capability means typed Stop submission is supported; it does not promise that a running child is currently available. Stop does not change RuntimeGeneration or establish readiness.

#### Typed Restart

`POST /v1/runtime/operations/restart` accepts only the same common mutation envelope as Start and Stop. The endpoint fixes the operation type to `restart`; its typed payload is empty, and its logical request identity is `runtime.restart/v1:{}` with a SHA-256 journal fingerprint. The caller cannot provide child operation IDs, a PID, signal, force flag, grace or timeout policy, executable, arguments, environment, working directory, shell command, or generic action parameters.

A new Restart requires a confirmed running child that the current `cpaprocess.Manager` owns. The Supervisor reserves that exact child before recording intent; `not_started`, confirmed `exited`, unknown state, unconfirmed ownership, or another active reservation returns `409 operation_state_conflict` before writing intent. Restart is one operation, not a composition of separately replayable public Stop and Start operations.

After accepted intent and running evidence are durably committed, Restart uses the Supervisor-owned execution context to terminate and confirm `Wait`/reap of that exact child, release its reservation, and spawn the Supervisor-local executable as the replacement. The shared lifecycle serialization prevents another Start, Stop, or Restart from crossing that sequence. A natural exit racing termination may proceed to replacement spawn only after the reserved child is confirmed reaped. Restart `succeeded` means confirmed old-child reap plus replacement spawn and ownership publication; it does not establish readiness or populate observed CPA version.

Termination failure does not attempt replacement spawn and records stable failure code `process_restart_stop_failed` when possible. Replacement spawn failure records `process_restart_start_failed` and leaves the old child stopped. A terminal persistence failure does not roll back either completed side effect. Retained `accepted`, `running`, `succeeded`, or `failed` Restart evidence is replayed without resuming or repeating termination or spawn. Restart does not change RuntimeGeneration or imply readiness; only a newly executed Restart whose terminal `succeeded` result is durable grants the fresh bounded recovery lease defined below.

Unix Domain Sockets and Windows Named Pipes are deferred. They may later be introduced as transport adapters without changing protocol semantics.

### 5. Supervisor operation journal

Runtime Supervisor uses its own small local SQLite database for durable operation state.

The journal is local runtime state, not product data. It MUST NOT be placed on shared/NFS storage and MUST NOT contain Manager product configuration, analytics, or long-lived product secrets.

Privileged mutations obey:

> durable intent before side effect

If an operation cannot be durably recorded, Supervisor MUST NOT perform binary replacement, process switching, rollback mutation, or other privileged filesystem side effects.

The initial journal needs only operation-oriented fields such as operation ID, type, Runtime identity, creation generation, enough typed-request data to detect conflicting operation ID reuse, expected/current target version, state, timestamps, rollback reference, and structured error code. Records created under an earlier generation MUST NOT be deleted or excluded from idempotency lookup merely because Supervisor starts with a new generation; recovery and observation of those records are later tasks.

### 6. Secret ownership

CPA Management Key remains a Manager-owned product secret and is stored using Manager encrypted storage.

For Embedded provisioning or mutation, Manager sends only the minimum execution material required by the current authenticated operation. Supervisor may hold that material in memory and apply it to CPA runtime configuration, but it MUST NOT become the long-term authority for the secret.

Secrets MUST NOT be written to the Supervisor operation journal, progress events, or logs.

The Manager-to-Supervisor Runtime transport credential is internal CPAMP installation state, not an ordinary user setting and not the CPA Management Key. Embedded packaging MAY bootstrap a high-entropy credential into a narrow, dedicated secret source shared only with Manager and Supervisor. It MUST preserve an existing value across restart/recreate and MUST NOT share Manager databases, `data.key`, Supervisor journal, or Gateway state to distribute that credential. Ingress does not receive it.

### 7. Readiness and crash-loop fencing

Lifecycle mutation success is separate from readiness: Start succeeds at spawn and ownership publication, Stop at confirmed exact-child reap, and Restart at old-child reap plus replacement spawn and ownership publication. These operations do not wait for readiness or write readiness into their journal evidence.

Authenticated `GET /v1/runtime/status` computes Embedded readiness on demand from the same Supervisor-owned process manager. A child is ready only when it is observed running both before and after a successful probe of its expected local listener/status route, with both observations referring to the same child instance. Supervisor-local, non-reusable in-memory child instance identity fences stale probe results across exit and replacement, including PID reuse. It is not persisted, exposed as protocol authority, substituted for an exact Stop target, or used as RuntimeGeneration.

The current safe probe is one bounded `HEAD http://<CPAMP_RUNTIME_CPA_ADDR>/healthz` request, accepting only HTTP 200. The Supervisor-private address defaults to `127.0.0.1:8317` and must be a loopback literal or `localhost` with a valid port. The path is fixed, environment HTTP proxies are disabled, and redirects are not followed. Handshake and unauthorized status requests do not probe. Probe failures are observed unready conditions, not Runtime Protocol server errors; observation does not write the journal or change lifecycle state, Runtime identity/generation, or capabilities.

| Observation | Runtime state |
|---|---|
| Read-only Supervisor, or unconfirmed child ownership | `unknown` |
| Not started, or confirmed exited/reaped | `offline` |
| Owned child running, but local probe has not succeeded for that same instance | `starting` |
| Same owned child running before/after a local `/healthz` HTTP 200 | `ready` |

Readiness MUST NOT use provider/model requests, acquire or retain the Manager-owned CPA Management Key, or intentionally make failed Management API authentication attempts. Missing or incorrect Management keys can trigger CPA IP bans; those failures and Management response version headers are not readiness evidence.

`CPAObservedVersion` is an optional observed fact, including for a ready Embedded runtime. The current safe `/healthz` route provides no running version, so Supervisor leaves it empty rather than substituting configured or expected values. Version matching becomes a readiness gate only when a future operation/configuration contract supplies both a trusted expected version and a safe observed-version source. External may retain its adapter-specific authenticated Management API version requirement.

Provider availability, quota state, credential cooling, or upstream network failure MUST NOT make the CPA process itself "not ready". These belong to gateway/provider health.

Automatic recovery is Supervisor-private execution policy, not a public lifecycle endpoint, capability, or Manager desired-state setting. It applies only to the exact Supervisor-owned child whose real `Wait`/reap path confirms an unexpected exit. `StateUnknown`, PID lookup, process scanning, readiness `starting`, `/healthz` timeout/refusal/non-200 response, CPA Home heartbeat failure, and provider/model/credential health are not recovery triggers. A readiness failure alone MUST NOT cause Supervisor to terminate or restart a running child.

A new process-local recovery epoch is armed only after an explicitly executed Start or Restart has a durable terminal `succeeded` result. The epoch is bound to the spawned in-memory child instance identity and has exactly three automatic spawn attempts. Each attempt waits a fixed one second, enters the same lifecycle serialization gate, and rechecks shutdown admission, lifecycle epoch, exact crashed instance, confirmed exited observation, current ownership, and remaining budget before writing intent or spawning. Each actual attempt consumes one budget unit after durable running evidence and immediately before spawn. Automatic spawn or readiness success never resets the budget; only a later explicit Start or Restart with durable terminal success creates a fresh three-attempt epoch.

Every automatic spawn uses a fresh high-entropy private operation ID and the operation type `auto_recovery`, with secret-free request fingerprint `SHA256("runtime.auto-recovery/v1:{}")`. Durable accepted intent and running evidence MUST both commit before the spawn side effect. Spawn failure records `process_recovery_start_failed` when terminal persistence remains available. Begin or running persistence failure performs no spawn. Terminal persistence failure after spawn does not terminate the replacement or retry the same attempt. Persistence ambiguity and budget exhaustion disable automatic authority and enter manual intervention.

Explicit Stop disables recovery after durable running evidence and before expected exact-child termination. Restart does the same for its old child; only its durably succeeded replacement establishes a new epoch. Exact child instance plus lifecycle epoch fence late exit notifications, stale timers, replacement ABA, and PID reuse. Fast exits that race terminal operation persistence are reconciled by re-observing the exact spawned instance after durable success, without relying solely on notification delivery.

Authenticated Runtime status may expose the recovery observation `inactive`, `armed`, `recovering`, or `manual_intervention` with attempts remaining in `0..3`. This observation is orthogonal to Runtime availability state and has no side effect. Shutdown closes recovery admission with public lifecycle admission, cancels pending timers, rechecks closure after acquiring the shared gate, and drains an already durably accepted automatic operation before closing the journal. Supervisor restart creates no recovery lease, restores no historical lease, and does not auto-start or adopt a child from prior journal evidence.

### 8. Platform topology

The logical architecture is identical across platforms. Platform differences are deployment adapters only.

#### Docker Embedded — Phase 1 priority

```text
cpamp-ingress container
  -> public :18317 transport mux

cpamp-manager container
  -> Manager internal :18317

cpamp-runtime container
  -> Runtime Supervisor (PID 1, internal :9081)
      -> CPA child process (internal :8317)
```

Only Ingress publishes a host port by default. Manager data and Runtime data use separate storage ownership. Runtime state is internally divided between Supervisor journal state and Gateway work/config/auth/log/plugin state. Ingress mounts neither data set. Runtime Supervisor does not require Docker socket access to manage CPA.

Docker Phase 1 has three deployment/container failure domains:

1. `cpamp-ingress` edge container.
2. `cpamp-manager` container.
3. `cpamp-runtime` container.

Within `cpamp-runtime`, Runtime Supervisor and CPA remain separate processes with distinct roles, ownership, state, and authority, but they share the Runtime container failure domain. Phase 1 does not guarantee that the CPA child survives Runtime Supervisor PID 1 exit or Runtime container crash, stop, or kill.

Phase 1 does not introduce a separate CPA container, Docker socket orchestration, or another lifecycle sidecar/controller. The Ingress failure domain exists only to provide the unified public L7 transport edge.

#### Native Linux

```text
systemd: CPAMP Manager
systemd: CPAMP Runtime Supervisor
         -> CPA child process
```

#### Native macOS

```text
launchd: CPAMP Manager
launchd: CPAMP Runtime Supervisor
         -> CPA child process
```

#### Native Windows

```text
Windows Service: CPAMP Manager
Windows Service: CPAMP Runtime Supervisor
                 -> CPA child process
```

Full Docker Embedded is the Phase 1 delivery target. Full Native Embedded follows after Runtime Foundation is stable and is not a Phase 1 start blocker.

Future Native topology may provide different OS/service failure and survival semantics. Docker Phase 1 does not depend on those semantics.

### 9. Failure-domain requirements

The architecture distinguishes application behavior invariants from deployment survival guarantees.

Application behavior invariants:

- Manager crash, restart, or temporary unavailability MUST NOT cause the control path to intentionally terminate a healthy CPA gateway. While Ingress and the Runtime container remain healthy, model traffic continues through `AI Client -> Ingress -> CPA/Gateway -> Provider` without traversing Manager or Supervisor.
- CPA child crash, exit, or readiness failure MUST NOT make Manager Console/API unavailable. Manager MUST be able to eventually observe and report the CPA runtime condition; the concrete lifecycle states are defined by later lifecycle work.
- Runtime Supervisor MUST NOT intentionally terminate a healthy CPA merely because Manager disconnects, a status request fails, ordinary reconciliation fails, or the control path has a transient failure.
- Manager health MUST be independently observable from CPA runtime health.

Deployment survival guarantee:

- Manager container failure MUST NOT stop an otherwise healthy Runtime container. Docker Phase 1 treats Manager and Runtime as separate deployment/container failure domains; this does not claim independence from a shared host or container-engine failure.
- Runtime/Gateway container failure MUST NOT make an otherwise healthy Manager route unavailable through a healthy Ingress.
- Ingress failure makes the unified public endpoint unavailable without merging Manager and Runtime ownership or process lifecycles.
- Docker Phase 1 does not guarantee CPA child survival after Runtime Supervisor PID 1 exits or the Runtime container crashes, stops, or is killed. Supervisor and CPA share that deployment failure domain.

Logical ownership, security/authority, state ownership, process role, and deployment failure domain are separate architectural dimensions. Sharing the Runtime container failure domain does not merge ownership: Runtime Supervisor remains the Execution Plane, and CPA remains the Data Plane.

### 10. Update ownership

- Manager decides update recommendation/channel/policy.
- Supervisor executes exact-version CPA runtime operations, staging, switch, readiness verification, and rollback.
- Manager and CPA updates are independent recovery domains; `Update All` is orchestration, not one atomic rollback transaction.
- Supervisor self-replacement is not part of the first Runtime Foundation implementation. Supervisor is updated by the outer CPAMP package/container/install mechanism.
- A build-time checksum-pinned embedded CPA artifact establishes image contents; it does not grant Runtime, Ingress, or container bootstrap updater authority.

## Phase 1 implementation boundary

Phase 1 MUST establish:

- `RuntimeClient` application port.
- Embedded and External adapters.
- Runtime Supervisor executable boundary.
- Runtime Protocol v1 handshake/status/capability slice.
- Supervisor private durable operation journal.
- Full Docker Embedded lifecycle foundation.
- Stateless single-public-port Ingress and unified Embedded Compose wiring.
- Build-time pinned embedded engine packaging with isolated Manager and Runtime storage.
- Tests enforcing failure-domain and storage-ownership invariants.

Phase 1 does NOT require:

- Full Public API v2.
- Migration Wizard or new-install UI.
- Full Native Embedded lifecycle.
- Unix socket / named-pipe transport.
- Supervisor self-update.
- Key-to-Credential hard routing.
- Gateway Governance / policy enforcement.
- Provider/model requests as readiness checks.
- Full runtime updater in the first skeleton PR.

## Consequences

This design introduces separate Supervisor and Ingress process roles in Embedded mode, and keeps both intentionally narrow. The benefit is an explicit execution boundary plus one public product endpoint without making Supervisor another product backend or putting model traffic through Manager.

HTTP/JSON is chosen for Phase 1 implementation speed, portability, testability, and observability. The protocol semantics are independent from transport so a stronger local IPC transport can be added later if required.

A separate local SQLite journal adds a small persistence component, but avoids unsafe ad-hoc JSON persistence and prevents Supervisor from depending on Manager databases.

## Non-negotiable invariants

1. Client model traffic traverses only Ingress and CPA/Gateway before its provider; it never traverses Manager or Supervisor.
2. Supervisor never opens Manager databases.
3. Supervisor is not product configuration or long-lived secret authority.
4. Manager may access CPA Management API directly; Supervisor is lifecycle execution, not a management proxy.
5. Embedded and External share the same application-level RuntimeClient contract.
6. Privileged runtime side effects require durable operation intent first.
7. Docker Phase 1 keeps Ingress, Manager, and Runtime as separate deployment failure domains; only Ingress publishes the default host port, while Supervisor and CPA keep distinct roles, ownership, state, and authority within the shared Runtime container failure domain.
8. Before mutation capability is enabled, each Runtime Supervisor process incarnation must establish a new authority epoch by freshly sampling an opaque random generation; Manager only observes and echoes it.
9. Mutations fence both Runtime identity and generation before idempotency resolution, durable intent, or side effects.
10. For one Runtime identity, durable operation ID idempotency spans Supervisor generations: replaying the same logical request never executes its side effect twice, and conflicting reuse fails closed.
11. Ingress owns no product configuration or lifecycle authority, persists no product state, and receives no Runtime, Manager, CPA Management, or provider credential.
12. Container/bootstrap startup may create internal transport and seed state, but MUST NOT infer desired running state or start CPA outside a typed durable lifecycle mutation.
