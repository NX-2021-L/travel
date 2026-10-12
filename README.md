# io-travel

## MCP auth (FLY-TSK-022, DVP-PLN-016 W2.1)

`backend/lambda/travel_mcp` is fronted by `io_kit_mcp_edge` (Pattern B, the same as io-finance), installed from
CodeArtifact `io/io-kit` (`io-kit-mcp-edge==0.1.0`, see `deploy.sh`). The edge serves the protected-resource and
authorization-server metadata and a secretless `/register` facade, and gates every other path with a validated
io-graph bearer token (public PKCE client `io-graph-pwa-iograph`, authorization server: the pool prefix domain, reached through the edge's `/authorize` redirect that drops `resource`, which Cognito managed login v2 cannot redeem; DVP-PLN-016 F4b).

Removed: `/authorize`, `/callback`, `/oauth/token`, `/oauth/register`, the old metadata handlers, `COGNITO_CLIENT_SECRET`
and every other confidential-client use (this closed an anonymous `/oauth/register` that returned the client secret),
and the static `MCP_API_KEY` bearer. `deploy.sh` strips those variables from the function environment.

Authorization is unchanged: `IOGRAPH_ALLOWED_SUBS` (edge path) and `COGNITO_ALLOWED_SUBS` (Enceladus-pool internal door,
validated with `validate_bearer`), plus `TRAVEL_WRITE_GROUPS`.

Follow-up for the orchestrator: after deploy, retire the old confidential Cognito client (the one `COGNITO_CLIENT_ID`
pointed at). The Enceladus-pool internal door is bound to that client, so it stops working when the client is retired.
