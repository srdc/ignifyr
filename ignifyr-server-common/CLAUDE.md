# ignifyr-server-common — shared server infrastructure

Shared Akka-HTTP plumbing used by `ignifyr-server` (and any other server build). Package root
`io.ignifyr.server.common`. Small and foundational — changes here ripple to every server endpoint,
so keep them backward-compatible.

- `config/WebServerConfig` — host / port / base-uri, built from the `webserver` HOCON block.
- `interceptor/ICORSHandler` — CORS directive trait mixed into endpoints.
- `interceptor/IErrorHandler` — maps exceptions → HTTP responses (pairs with `model/IgnifyrError`).
- `model/IgnifyrError` — common error payload; `model/IgnifyrRestCall` — per-request context.

No tests live here; it is exercised through `ignifyr-server`.
