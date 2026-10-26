# agentgateway in front of the mesh

Everything the guide at `site/content/docs/guides/agentgateway.md` walks
through, as one `docker compose` project and a script that checks each step:

- Keycloak 26.4.0 with the realm `mesh` (client `agentmesh`, an audience
  mapper so access tokens carry `aud: agentmesh`, `alice` in group `devs`,
  `bob` in group `ops`) and a second realm, `elsewhere`;
- agentgateway v1.6.0 ([`agentgateway.yaml`](agentgateway.yaml)), which
  validates each caller's access token and exchanges it at a shared
  `agentmesh-node` for a mesh credential of that caller;
- `agentmesh-control-plane` with the realm as its issuer, one router, the
  calculator of [`../calc-mcp`](../calc-mcp) published as `mcp://calc`, and a
  header echo (`echo.py`) behind the same agentgateway policies.

```sh
./run.sh        # bring it up, run every case, exit non-zero on a mismatch
./run.sh down   # remove the containers and the generated tokens
```

`run.sh` prints one line per case:

```
== Calling mcp://calc through agentgateway
  PASS  alice (calc-user through group:devs)                               200
  PASS  bob (no calc-user), refused by the provider                        403

== Refused at agentgateway
  PASS  no token                                                           401
  PASS  not a token                                                        401
  PASS  alice, token for client other-app (audience)                       401
  PASS  alice, token from realm elsewhere (issuer)                         401

== What a backend behind agentgateway receives
  PASS  the exchanged credential in x-mesh-authentication                  present
  PASS  the caller's token in authorization                                absent

== A token through its expiry (60s lifetime in this realm)
  PASS  alice, before exp                                                  200
  PASS  alice, 20s after exp: accepted by agentgateway, refused at the exchange 400
  PASS  alice, 80s after exp: refused by agentgateway                      401
  PASS  alice, a new token                                                 200
```

The agentmesh images are pinned to the commit the example was tested with.
`AGENTMESH_TAG` selects another published commit or release, and
`AGENTMESH_REGISTRY` another registry.

`.run/` holds the admin, API and bootstrap tokens `run.sh` generates on each
run, and is not committed. The users' passwords in the realm files are for
this example only. Keycloak runs `start-dev` over plain `http` inside the
Compose network; the guide lists what to change for a real deployment.
