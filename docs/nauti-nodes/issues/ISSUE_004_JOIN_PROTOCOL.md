# Issue 004 — Join Protocol

## Objective

Implement the Nauti lineage join flow. A joining node sends a join request
with a parent reference. The coordinator (Node 1) validates, records the
relationship, and returns bootstrap information.

## Original State

No join protocol. Nodes registered via flat `POST /internal/worker/register`
with no parent/child concept.

## Changes

- Lineage store wired into coordinator startup (`coordinator.rs`)
- Root node auto-registered from identity at startup
- `POST /v1/node/join` endpoint: validates parent exists, prevents cycles,
  self-parenting, duplicate identity; returns relationship with depth/branch
- `GET /v1/tree/snapshot` endpoint: returns full active lineage tree
- `LineageStore` added to `AppState`
- `JoinRequest` struct with `node_id`, `parent_node_id`, `public_key_hex`,
  `software_version`, `capabilities`

## Live Evidence

```
ROOT=66b44dc2-1e87-41dc-9fed-90bf40502234

JOIN child-1 under root:
  {"branch_id":"branch-child-1","depth":1,"node_id":"child-1",
   "parent_node_id":"66b44dc2...","root_node_id":"66b44dc2...","status":"active"}

JOIN child-2 under root:
  {"branch_id":"branch-child-2","depth":1,"node_id":"child-2",...}

JOIN grandchild-1 under child-1:
  {"branch_id":"branch-child-1","depth":2,"node_id":"grandchild-1",
   "parent_node_id":"child-1",...}

PREVENT SELF-PARENT: "cannot parent a node to itself" (409)
PREVENT DUPLICATE: "node child-1 already has an active parent" (409)
```

## Remaining Limitations

- No authentication on join endpoint (anyone can join any tree)
- No challenge/response auth yet
- No `GET /v1/tree/events` for incremental updates
- No forwarding through parent to root (direct join to root only)

## Disposition

PASS — join protocol works with cycle/self-parent/duplicate protection.