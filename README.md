# io-travel

## MCP auth (FLY-TSK-022, DVP-PLN-016 W2.1)

`backend/lambda/travel_mcp` is fronted by `io_kit_mcp_edge` 0.2 (`EdgeConfig(connector="io-travel")`, io-finance pattern), installed from
CodeArtifact `io/io-kit` (`io-kit-mcp-edge==0.2.1`, see `deploy.sh`). The fleet manifest supplies the base URL, the `/mcp` mount, the issuer, the
client id and the sign-in service (`https://connect.thepup.io`). The edge serves only the protected-resource metadata (resource
`https://travel.thepup.io/mcp`) and gates every other path with a validated io-graph bearer token. `/register`, `/authorize` and the
authorization-server metadata belong to the sign-in service and answer 401 here. MCP answers at `/mcp` (canonical) and, until io reconnects,
still at `/`. The CloudFront distribution has only a default behavior, so `/mcp` and `/.well-known/*` reach the Lambda with no extra routing.

Removed: `/authorize`, `/callback`, `/oauth/token`, `/oauth/register`, the old metadata handlers, `COGNITO_CLIENT_SECRET`
and every other confidential-client use (this closed an anonymous `/oauth/register` that returned the client secret),
and the static `MCP_API_KEY` bearer. `deploy.sh` strips those variables from the function environment.

Authorization is unchanged: `IOGRAPH_ALLOWED_SUBS` (edge path) and `COGNITO_ALLOWED_SUBS` (Enceladus-pool internal door,
validated with `validate_bearer`), plus `TRAVEL_WRITE_GROUPS`.

Follow-up for the orchestrator: after deploy, retire the old confidential Cognito client (the one `COGNITO_CLIENT_ID`
pointed at). The Enceladus-pool internal door is bound to that client, so it stops working when the client is retired.
