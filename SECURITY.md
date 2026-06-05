# Security Policy

## Reporting vulnerabilities

If you believe you have found a security vulnerability in `koanf-etcd`, please report it privately rather than via a public issue. The preferred channel is a GitHub Security Advisory: open one at
<https://github.com/uded/koanf-etcd/security/advisories/new>.

We aim for a best-effort first response within a few business days. We are not a 24/7 commercial security team, so please do not treat the timing as an SLA — but every report is taken seriously and triaged. If your finding requires coordinated disclosure with downstream consumers we are happy to work with you on the timeline.

When reporting, the following helps:

- A minimal reproduction (option combination, etcd version, Go version).
- Affected version range, if known.
- Your assessment of the impact (data exposure, DoS, privilege escalation, etc.).

## Threat model — what this library defends against

- **Watch event values are not logged by default.** The library does not write `Event.Value` (or any payload byte) into structured-logging output. Callbacks receive the bytes; what the consumer does with them is the consumer's policy decision.
- **TLS 1.2 floor for file-loaded configs.** `WithTLSFiles` constructs a `*tls.Config` with `MinVersion = TLS 1.2`. TLS 1.0 / 1.1 are never negotiated through that path. Callers wanting TLS 1.3 only can pass a hand-built `*tls.Config` via `WithTLS`.
- **Watch-error classification gates retry storms.** The watch loop classifies etcd errors (transient, compaction, auth, fatal) and feeds the class to `WithOnWatchError`. Auth and fatal failures stop the loop instead of hot-spinning reconnects.
- **`DeletePrefix("")` guard.** The `etcdwrite` subpackage refuses an empty prefix to `DeletePrefix`, eliminating a "delete the entire keyspace" footgun in calling code.
- **Graceful `Close()` drain.** `Close()` cancels the watch context, waits for the watch goroutine (with a bounded timeout via `WithCloseTimeout`), and only then tears down the built-in client. BYO clients are never closed by the Provider — lifecycle stays with the caller.

## Threat model — what this library does NOT defend against

- **Compromised DNS for SRV discovery.** `WithEndpointsFromSRV` accepts the endpoints DNS returns at face value. A network-positioned attacker who can intercept or spoof unencrypted DNS responses can redirect the etcd client to any host of their choice. The library does not validate the SRV answer in any cryptographic way.

  **Mitigation:** enable TLS (`WithTLS` or `WithTLSFiles`) *and* set `WithTLSServerName` to a name present in your etcd server certificate's SAN. The TLS handshake will then fail against any attacker-supplied host that doesn't carry a matching cert. For stronger guarantees, use a DNSSEC-validated resolver, or skip SRV entirely and supply explicit endpoints via `WithEndpoints`.

- **Watch event payload trust.** `Event.Value` — delivered through `WatchTyped` — is raw, unvalidated, attacker-controllable bytes from etcd. Any process able to write to your etcd cluster can put any bytes there. Consumers MUST validate before passing the value to:

  - shell commands (command injection),
  - SQL queries (SQL injection),
  - log lines that are subsequently rendered as HTML (XSS),
  - file paths (path traversal),
  - or anywhere else a malicious payload could escape its context.

  The library deliberately does NOT apply the configured value transform to typed events: consumers that parse JSON, protobuf, or other structured payloads do not want a `TrimSpace`-and-stringify round-trip injected into their pipeline. The read path (`Read` / `ReadBytes`) does apply the transform. The implication is that trust over watch payloads is the caller's responsibility.

- **Credential lifetime in memory.** `WithAuth("user", "pass")` holds the static strings on the Go heap for the Provider's lifetime. They will appear in core dumps and heap snapshots. For rotation or ephemeral credentials, prefer:

  - `WithAuthProvider(fn)` — fetches credentials just before client construction so the caller can keep them in a buffer that they zero out immediately afterwards, or
  - `WithClient(yourClient)` — hand the Provider a fully-constructed `*clientv3.Client` and manage its lifecycle (including rotation) on the caller's side.

  Note that the etcd `clientv3` library retains credentials internally for token-refresh against etcd's auth server, so `WithAuthProvider` does not give you live rotation of an *active* connection — only of the credential string the Provider itself holds. For live rotation on an active connection, BYO-client is the only option.

- **etcd cluster compromise.** If an attacker controls your etcd cluster they can deliver arbitrary configuration to every consumer. The library assumes etcd is a trusted source of truth. Production etcd deployments should sit behind mTLS and RBAC, with network segmentation that limits who can talk to the cluster at all.

## TLS posture

`loadTLSFromFiles` (used by `WithTLSFiles`) pins `MinVersion` to TLS 1.2. Callers who want TLS 1.3-only behavior should build their own `*tls.Config` and pass it through `WithTLS`, setting `MinVersion: tls.VersionTLS13`.

`WithTLSServerName` is required when connecting to etcd endpoints by raw IP address. Without it, the etcd server certificate's SAN must include the literal IP, which is uncommon in real deployments. Set the server name to a DNS name in the certificate's SAN list and the TLS handshake will validate against that name regardless of how you addressed the endpoint.

## Supply chain

- Releases are GPG-signed git tags.
- Each release artifact set on GitHub includes an SPDX SBOM, so downstream consumers can audit the dependency tree of a specific release.
- `govulncheck` is part of the CI gate; a PR cannot land with known callable vulnerabilities in the dependency graph.
- The main module dependency tree is kept lean. The test harness — which pulls in `go.etcd.io/etcd/server/v3` and its substantial transitive set — lives in a nested module under `tests/integration`, so library consumers do not inherit that surface.
