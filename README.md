# a2a-interconnect

**[🇯🇵 日本語版はこちら / Japanese version](README.ja.md)**

An A2A (Agent2Agent) implementation of the **Connection Coordinator API** — the open, symmetric OpenAPI 3.0 specification published by AWS/Google to coordinate managed L3 interconnects between cloud providers ([aws/Interconnect](https://github.com/aws/Interconnect)).

**Specification compliance is the primary goal of this project.** The deterministic vendor tools it calls into (see below) are a *means* to that end, not the end itself — they exist so that a negotiated connection is realized on real network equipment without the non-determinism that natural-language-driven configuration otherwise introduces.

## What this is

Two organizations (a *Negotiator* and a *Responder*) each run one instance of this agent, representing their own side of an interconnect. The agents exchange a small set of A2A messages to:

1. Establish a connection request and issue an `ActivationKey`
2. Have the customer (or an automated agent acting on the customer's behalf) carry that key to the peer
3. Verify the key's authenticity (peer-to-peer, not customer-mediated)
4. Negotiate a concrete VLAN / ASN / subnet / MTU configuration from a range the Responder offers
5. Deploy the agreed configuration to each side's own network device
6. Confirm the connection is up

Natural-language input is supported at exactly one point — the initial "create a connection" request. Everything after that point is fully structured, deterministic data; no LLM is involved in parameter negotiation, VLAN/ASN/subnet selection, or device configuration generation.

## Architecture

This repository sits in the **Integration Layer**: it negotiates the interconnect protocol and decides *what* configuration is needed, but it does not talk to network devices directly. Actual device configuration is delegated to a **Vendor Core** — a separate service that owns the NETCONF session, vendor-specific XML generation, and its own policy/safety checks for a given NOS (e.g. Junos, Arista EOS).

```
Negotiator (this repo)  <──A2A/HTTP──>  Responder (this repo)
        │                                        │
        ▼ /deploy_structured                     ▼ /deploy_structured
  Vendor Core (Junos, Arista, ...)          Vendor Core (Junos, Arista, ...)
        │                                        │
        ▼ NETCONF                                ▼ NETCONF
  Network device                            Network device
```

A Vendor Core is **not included** in this repository.

### The `/deploy_structured` contract

A Vendor Core exposes a single deterministic endpoint that this agent calls once per task in the deployment DAG (see [Design notes](#design-notes)):

```
POST /deploy_structured
request:
  {
    "task_type": str,   # selects which XML template / governance
                         # classification to apply on the Vendor Core side
    "params":    dict,  # task_type-specific structured parameters
    "device": {"ip": str, "port": str, "username": str, "password": str}
  }
response:
  { "status": "success" | "dry_run" | "no_change" | "all_success" | "failure" | ..., ... }
```

No natural language and no LLM are involved on either side of this call — `task_type` and `params` are already fully resolved structured data by the time this agent sends them. A Vendor Core is expected to turn them into vendor-specific configuration (e.g. NETCONF XML), apply its own policy checks, deploy in a single commit, and report back a result whose `status` field can be interpreted as success/failure. Adding support for a new NOS is a matter of implementing this one endpoint; no changes to this repository are required.

## Included files

| File | Role |
|---|---|
| `interconnect_bridge_a2a_server.py` | The agent itself: A2A skills, negotiation state machine, deterministic task-DAG generation, deployment orchestration |
| `governance_client.py` | Thin HTTP client for an optional external policy/governance service. Fails safe: write actions are denied and read actions are permitted if the governance service is unreachable, so this agent runs standalone without it |
| `llm_factory.py` | Shared LLM initialization/fallback helper (Groq primary, Gemini via Vertex AI fallback; used only for the natural-language intent-extraction skill) |
| `response_schema.py` | Small helpers for consistent success/error response shapes |

## Requirements

```bash
pip install a2a-sdk fastapi uvicorn httpx pydantic langchain-openai
# optional, only needed for the Gemini fallback in llm_factory.py:
pip install "langchain-google-vertexai>=3.2.4"
```

Gemini access is via Vertex AI, not a standalone Gemini API key: it requires a GCP project and Application Default Credentials (`gcloud auth application-default login`) or a service account with `roles/aiplatform.user`. If `langchain-google-vertexai` is not installed or GCP credentials are not configured, the agent runs fine on Groq alone.

An external governance/policy service is optional (see `governance_client.py` above).

## Configuration

All configuration is via environment variables.

| Variable | Default | Meaning |
|---|---|---|
| `A2A_PORT` | `8202` | Port this agent listens on |
| `PROVIDER_ID` | `aws` | This agent's own identity (e.g. `aws`, `gcp`) |
| `VENDOR_ID` | `junos` | Which Vendor Core this side deploys through (`junos` \| `ceos`) |
| `DEVICE_IP` / `DEVICE_USERNAME` / `DEVICE_PASSWORD` | `172.20.100.31` / `admin` / `admin` | Credentials passed to the Vendor Core for this side's device |
| `JUNOS_IFACE` | `et-0/0/2` | Physical interface name (Junos Vendor Core) |
| `CEOS_IFACE` | `Ethernet1` | Physical interface name (Arista Vendor Core) |
| `PEER_BASE_URL_<PEER_PROVIDER>` | — | Base URL of the peer's agent, keyed by the **peer's** identity, not your own (e.g. `PEER_BASE_URL_GCP=http://localhost:8290` on the `aws` side) |
| `PROVIDER_ASN_<PROVIDER>` | — | Each provider's real BGP ASN, keyed by that provider's identity. Needed on both sides for every provider involved in a negotiation (e.g. `PROVIDER_ASN_AWS=65001 PROVIDER_ASN_GCP=65002`) |
| `CEOS_HUB_URL` / `JUNOS_HUB_URL` | `http://localhost:8000` / `http://localhost:8020` | Vendor Core Hub endpoints |
| `GOVERNANCE_A2A_URL` | `http://localhost:8190` | Optional external governance service |
| `GROQ_API_KEY` | — | Required for the natural-language intent-extraction skill |

## Skills

| Skill | Role |
|---|---|
| `create_connection` | Negotiator: issue an `ActivationKey` for a connection request given structured parameters |
| `create_connection_nl` | Same, but parameters are extracted from a natural-language request |
| `accept_connection_nl` | Responder: accept an `ActivationKey` brought by the customer (structured key/IDs; free text is not parsed for values) |
| `verify_activation_key` | Peer-to-peer key verification (Responder → Negotiator) |
| `generate_feature_guidance` | Peer-to-peer range negotiation (Negotiator → Responder) |
| `create_feature` | Propose/accept a concrete configuration and trigger deployment |
| `approve` | Human approval gate for deployments the governance layer flags for review |
| `notify_connection_status` | Peer-to-peer completion notification |
| `get_negotiation_status` | Query the current state of a connection |

## Design notes

- **Deterministic execution, LLM-free negotiation.** The only step that uses an LLM is turning free text into `{environment, bandwidth_mbps, peer_provider}` for `create_connection_nl`. VLAN/ASN/subnet selection, XML generation, and device deployment are all plain code with no model involved — this was a deliberate response to observed non-determinism when device configuration was generated by an LLM.
- **The Responder offers a range; the Negotiator selects from it**, matching the spec's intended division of responsibility, rather than one side dictating final values to the other.
- **A capability/DAG layer** (`CAPABILITY_REGISTRY` / `CEOS_CAPABILITY_REGISTRY`) declares what each vendor's deployment needs and the dependencies between those steps; the actual execution order is derived from that declaration rather than hard-coded, and adding a new vendor is a matter of writing one new registry rather than new orchestration logic.
- **Real, provider-specific ASNs**, not per-connection allocated values, since AS numbers are fixed identifiers of a real network operator rather than something negotiated per connection.

## License

MIT — see `LICENSE`.
