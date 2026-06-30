# tofhir-server-common — shared server infrastructure

Shared Akka-HTTP plumbing used by `tofhir-server` (and any other server build). Package root
`io.tofhir.server.common`. Small and foundational — changes here ripple to every server endpoint,
so keep them backward-compatible. (Naming: `tofhir` ≡ Ignifyr — see root [CLAUDE.md](../CLAUDE.md).)

- `config/WebServerConfig` — host / port / base-uri, built from the `webserver` HOCON block.
- `interceptor/ICORSHandler` — CORS directive trait mixed into endpoints.
- `interceptor/IErrorHandler` — maps exceptions → HTTP responses (pairs with `model/ToFhirError`).
- `model/ToFhirError` — common error payload; `model/ToFhirRestCall` — per-request context.

No tests live here; it is exercised through `tofhir-server`.
