# ADR-0001: CPAMP 2.0 Runtime Foundation

- Status: Accepted
- Date: 2026-09-14
- Scope: Phase 1 Runtime Foundation

## Context

CPAMP 2.0 separates product control, privileged runtime execution, and model traffic into distinct ownership domains. The goal is to let the gateway keep serving model traffic when the control plane is unavailable, while keeping privileged process and update operations out of the Manager process.

This ADR freezes the minimum architecture needed to begin implementation. It intentionally does not freeze later Gateway Governance, Public API v2, migration UI, or provider-routing policy.

## Decision

### 1. Product and runtime roles

CPAMP uses three runtime roles:

- **Manager** — Control Plane. Owns desired product/runtime configuration, user/admin state, usage/analytics, update recommendation policy, and encrypted product secrets.
- **Runtime Supervisor** — Execution Plane for Embedded mode. Owns privileged lifecycle execution and its private operation journal. It does not own product configuration.
- **CPA** — Data Plane. Owns gateway request handling, provider/credential runtime behavior, and model traffic.

Model traffic MUST follow:

```text
AI Client -> CPA -> Provider
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

Invariant:

> Manager owns desired state; Supervisor owns privileged execution journal; CPA/Supervisor expose observed state; Manager reconciles desired and observed state.

Runtime Supervisor MUST NOT read, open, migrate, or depend on Manager SQLite databases or `data.key`.

### 4. Runtime Protocol v1

Phase 1 uses a versioned **HTTP/JSON** Runtime Protocol.

Transport placement:

- Docker Embedded: private Docker network only; no host publication by default.
- Native Linux/macOS/Windows: loopback by default.

The protocol MUST be authenticated per installation and MUST support explicit timeouts, stale-generation fencing, and operation IDs.

The first protocol slice contains only what Runtime Foundation needs:

- handshake
- protocol version
- runtime identity / generation
- capabilities
- status
- running CPA version
- lifecycle action envelope
- structured result/reason

Unix Domain Sockets and Windows Named Pipes are deferred. They may later be introduced as transport adapters without changing protocol semantics.

### 5. Supervisor operation journal

Runtime Supervisor uses its own small local SQLite database for durable operation state.

The journal is local runtime state, not product data. It MUST NOT be placed on shared/NFS storage and MUST NOT contain Manager product configuration, analytics, or long-lived product secrets.

Privileged mutations obey:

> durable intent before side effect

If an operation cannot be durably recorded, Supervisor MUST NOT perform binary replacement, process switching, rollback mutation, or other privileged filesystem side effects.

The initial journal needs only operation-oriented fields such as operation ID, type, runtime generation, expected/current target version, state, timestamps, rollback reference, and structured error code.

### 6. Secret ownership

CPA Management Key remains a Manager-owned product secret and is stored using Manager encrypted storage.

For Embedded provisioning or mutation, Manager sends only the minimum execution material required by the current authenticated operation. Supervisor may hold that material in memory and apply it to CPA runtime configuration, but it MUST NOT become the long-term authority for the secret.

Secrets MUST NOT be written to the Supervisor operation journal, progress events, or logs.

### 7. Readiness and crash-loop fencing

Runtime readiness is based on deterministic runtime conditions, not a real provider/model request.

A CPA runtime becomes ready only after the required local checks succeed, including:

1. CPA process is alive.
2. Expected listener is ready.
3. CPA management/status probe succeeds.
4. Running version matches the expected operation state when version is part of the operation precondition.

Provider availability, quota state, credential cooling, or upstream network failure MUST NOT make the CPA process itself "not ready". These belong to gateway/provider health.

Supervisor performs bounded recovery. Repeated exits before reaching readiness MUST eventually enter a structured crash-loop/manual-intervention state rather than restarting forever. Phase 1 begins with a simple bounded retry budget; complex adaptive recovery is out of scope.

### 8. Platform topology

The logical architecture is identical across platforms. Platform differences are deployment adapters only.

#### Docker Embedded — Phase 1 priority

```text
cpamp-manager container
  -> Manager

cpamp-runtime container
  -> Runtime Supervisor (PID 1)
      -> CPA child process
```

Manager data and Runtime data use separate storage ownership. Runtime Supervisor does not require Docker socket access to manage CPA.

Docker Phase 1 has two deployment/container failure domains:

1. `cpamp-manager` container.
2. `cpamp-runtime` container.

Within `cpamp-runtime`, Runtime Supervisor and CPA remain separate processes with distinct roles, ownership, state, and authority, but they share the Runtime container failure domain. Phase 1 does not guarantee that the CPA child survives Runtime Supervisor PID 1 exit or Runtime container crash, stop, or kill.

Phase 1 does not introduce a third CPA container, Docker socket orchestration, or another sidecar/controller to manufacture an additional deployment failure domain.

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

- Manager crash, restart, or temporary unavailability MUST NOT cause the control path to intentionally terminate a healthy CPA gateway. While the Runtime container remains healthy, model traffic continues directly through `AI Client -> CPA -> Provider` without traversing Manager.
- CPA child crash, exit, or readiness failure MUST NOT make Manager Console/API unavailable. Manager MUST be able to eventually observe and report the CPA runtime condition; the concrete lifecycle states are defined by later lifecycle work.
- Runtime Supervisor MUST NOT intentionally terminate a healthy CPA merely because Manager disconnects, a status request fails, ordinary reconciliation fails, or the control path has a transient failure.
- Manager health MUST be independently observable from CPA runtime health.

Deployment survival guarantee:

- Manager container failure MUST NOT stop an otherwise healthy Runtime container. Docker Phase 1 treats Manager and Runtime as separate deployment/container failure domains; this does not claim independence from a shared host or container-engine failure.
- Docker Phase 1 does not guarantee CPA child survival after Runtime Supervisor PID 1 exits or the Runtime container crashes, stops, or is killed. Supervisor and CPA share that deployment failure domain.

Logical ownership, security/authority, state ownership, process role, and deployment failure domain are separate architectural dimensions. Sharing the Runtime container failure domain does not merge ownership: Runtime Supervisor remains the Execution Plane, and CPA remains the Data Plane.

### 10. Update ownership

- Manager decides update recommendation/channel/policy.
- Supervisor executes exact-version CPA runtime operations, staging, switch, readiness verification, and rollback.
- Manager and CPA updates are independent recovery domains; `Update All` is orchestration, not one atomic rollback transaction.
- Supervisor self-replacement is not part of the first Runtime Foundation implementation. Supervisor is updated by the outer CPAMP package/container/install mechanism.

## Phase 1 implementation boundary

Phase 1 MUST establish:

- `RuntimeClient` application port.
- Embedded and External adapters.
- Runtime Supervisor executable boundary.
- Runtime Protocol v1 handshake/status/capability slice.
- Supervisor private durable operation journal.
- Full Docker Embedded lifecycle foundation.
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

This design introduces a third process role in Embedded mode, but keeps it intentionally thin. The benefit is an explicit execution boundary without making Supervisor another product backend or putting model traffic through CPAMP.

HTTP/JSON is chosen for Phase 1 implementation speed, portability, testability, and observability. The protocol semantics are independent from transport so a stronger local IPC transport can be added later if required.

A separate local SQLite journal adds a small persistence component, but avoids unsafe ad-hoc JSON persistence and prevents Supervisor from depending on Manager databases.

## Non-negotiable invariants

1. Client model traffic never traverses Manager or Supervisor.
2. Supervisor never opens Manager databases.
3. Supervisor is not product configuration or long-lived secret authority.
4. Manager may access CPA Management API directly; Supervisor is lifecycle execution, not a management proxy.
5. Embedded and External share the same application-level RuntimeClient contract.
6. Privileged runtime side effects require durable operation intent first.
7. Docker Phase 1 keeps the Manager and Runtime containers as separate deployment failure domains; Supervisor and CPA keep distinct roles, ownership, state, and authority within the shared Runtime container failure domain.
