# Proteus — Design & Architecture

## Overview

**Proteus** is a SAF-style CLI (`ptx`) for managing Scylla Cloud and X-Cloud clusters via the Scylla Cloud REST API. Single-binary Python, YAML-declarative, no local state.

### Design Principles

1. **Simplicity** — YAML config + small set of verbs (`setup`, `resize`, `destroy`, `status`, `list`, `validate`).
2. **Declarative** — Cluster shape lives in `config.yml`. CLI flags override per invocation.
3. **Focused** — Provisioning, resizing, deletion, status. No stress testing, no scylla.yaml tuning, no loaders.
4. **Cloud-native** — Direct REST against `api.cloud.scylladb.com`. AWS + GCP.
5. **Existing-cluster support** — `existing_cluster_id` attaches to clusters created elsewhere.
6. **Stateless** — Source of truth is Scylla Cloud. Every command re-queries the API.

---

## Comparison: Proteus vs SAF

| Feature | SAF | Proteus |
|---------|-----|---------|
| Scope | Full automation (deploy, stress, monitor) | Provisioning + resize + destroy |
| Cluster types | Enterprise, X-Cloud, Scylla Cloud | X-Cloud, Scylla Cloud |
| Infra layer | Terraform + Ansible | Scylla Cloud REST API |
| Stress testing | Integrated (Latte, cassandra-stress, …) | Out of scope |
| Scylla param tuning | Yes | No (Scylla Cloud defaults) |
| State | Terraform state file | None — API is source of truth |
| Target users | Internal SRE / Infra | External devs, ops teams |

---

## Architecture

### High-Level Flow

```
config.yml
    │
    ▼
┌───────────────────────────────┐
│  Config Parser (config.py)    │
│  - YAML load + env subst      │
│  - Path resolution / discovery│
└──────────────┬────────────────┘
               │
┌──────────────▼────────────────┐
│  ID Mapper (mapping.py)       │
│  - cloud-data.json lookup     │
│  - friendly name → API ID     │
└──────────────┬────────────────┘
               │
┌──────────────▼────────────────┐
│  CLI Dispatcher (cli.py)      │
│  - Subcommands + flag merging │
│  - Dry-run, wait/poll loops   │
│  - Status table rendering     │
└──────────────┬────────────────┘
               │
┌──────────────▼────────────────┐
│  API Client (api.py)          │
│  - Bearer auth                │
│  - Timeout / TLS verify       │
│  - HTTP error → APIError      │
└──────────────┬────────────────┘
               │
┌──────────────▼────────────────┐
│  Error Decoder (errors.py)    │
│  - api_error_codes.tsv lookup │
└───────────────────────────────┘
```

### Module Map

| Module | Responsibility |
|--------|----------------|
| `scm/cli.py` | Subcommand parsers, command handlers, output rendering, polling |
| `scm/config.py` | YAML loading, env-var substitution, config discovery |
| `scm/api.py` | `ScyllaCloudAPI` REST client (requests-based) |
| `scm/mapping.py` | `cloud-data.json` resolver — name → ID |
| `scm/errors.py` | `api_error_codes.tsv` decoder |

---

## Config Discovery

Resolution order (first match wins):

1. `--config <path>` flag
2. `$PROTEUS_CONFIG` env var
3. `~/.config/proteus/config.yml` (default — set by `install.sh` symlink)
4. Walk up from CWD looking for `config.yml` containing a `clusters:` key

Token resolution: `--api-token` → `$SCYLLA_CLOUD_API_TOKEN` → `api.token` in config.

---

## State Model

Proteus stores **no local state**. Each command:

1. Loads config.
2. Resolves friendly names to IDs via `cloud-data.json`.
3. Queries the API for current cluster state.
4. Diffs against desired config (resize) or executes the requested action.

The only mutation Proteus performs on the config file is writing back `existing_cluster_id` after a successful `setup` create.

### Resize as a Two-Phase Operation (X-Cloud)

X-Cloud resizes are policy updates, not direct node count changes. Proteus tracks both phases:

1. **Phase 1** — POST scaling policy update; poll request until `COMPLETED`.
2. **Phase 2** — Wait for the auto-scaler to enqueue a `RESIZE_CLUSTER_*` request; poll it to completion.

Scylla Cloud resizes go via the node-group resize endpoint directly.

---

## Reference Data

### `cloud-data.json`

Maps friendly names to API IDs.

Schema (current):

```
{
  "instances": {
    "<aws|gcp>": {
      "<region>": {
        "<instance_type>": {
          "provider_id": <int>,
          "region_id":   <int>,
          "id":          <int>     // instance type ID
        }
      }
    }
  }
}
```

Refresh from the API with `ptx cache-refresh-cloud`. Inspect with `ptx cloud-data --cloud <p> --region <r>`.

### `api_error_codes.tsv`

Tab-separated `code\tdescription` rows. On `APIError`, Proteus extracts the code from the response body and prints both code and description.

---

## CLI Design

### Subcommands

| Command | Purpose |
|---------|---------|
| `setup <id>`            | Create new or attach to existing cluster (writes back `existing_cluster_id`) |
| `resize <id>`           | Update X-Cloud scaling policy or Scylla Cloud node groups |
| `destroy <id> --yes`    | Delete cluster (`--yes` required) |
| `status [id]`           | Live cluster status table (all configured or single) |
| `list`                  | All clusters in the account (live from API) |
| `validate [ids...]`     | Validate config + mapping resolution offline |
| `cache-refresh-cloud`   | Refresh `cloud-data.json` |
| `cloud-data`            | Inspect mapped data for a region |

### Cross-Command Flags

- `--dry-run` — Print payload, no API call (setup, resize, destroy).
- `--wait` / `--no-wait` — Block until async request completes (default: wait).
- `--wait-timeout <s>` / `--poll-interval <s>` — Polling tuning.
- Cluster override flags (`--cluster-type`, `--cloud`, `--region`, `--cidr-block`, `--vcpu-min`, etc.) let callers run without (or in addition to) config.

### Status Symbols

| Symbol | Meaning |
|--------|---------|
| `[✔]` | ACTIVE / COMPLETED |
| `[◑]` | Transitional (CREATING, UPDATING, MODIFYING, …) |
| `[✘]` | ERROR / FAILED / DELETED |
| `[–]` | Unknown / not yet provisioned |

---

## Safety

- **In-progress guard** — Before mutating, Proteus checks for an outstanding request on the target cluster and aborts rather than stacking.
- **Destroy confirmation** — `destroy` requires `--yes`.
- **Dry-run** — `--dry-run` short-circuits before any state-changing API call.
- **TLS** — Verified by default. `--no-ssl-verify` available for dev environments.

---

## Security

### API Token

Resolution: `--api-token` → `$SCYLLA_CLOUD_API_TOKEN` → `api.token`. Env var preferred — keeps the token out of version-controlled files.

### Network

- `broadcast_type: PRIVATE` uses VPC peering; `PUBLIC` is internet-reachable.
- `cidr_block` must not overlap peers or other clusters.
- SSH keys (`api.ssh_key_public` / `api.ssh_key_private`) reference local paths; only the public key is sent to the API.

---

## Dependencies

- `PyYAML` — config parsing
- `requests` — HTTP client
- Python ≥ 3.10
- argparse (stdlib) — CLI

No async, no Terraform, no Ansible.

---

## When to Use Proteus vs SAF

**Proteus** — Scylla Cloud or X-Cloud provisioning, resizing existing clusters, simple YAML, no stress / tuning, single-region.

**SAF** — Self-managed enterprise clusters, full IaC, stress testing, multi-DC, scylla.yaml tuning.

**Both** — Provision with Proteus, exercise the cluster with SAF (via `existing_cluster_id`) or specialized tools (latte, espresso).

---

## Roadmap

- Multi-region clusters
- Backup / restore operations
- VPC peering management
- Status streaming (websocket / SSE if API exposes)
