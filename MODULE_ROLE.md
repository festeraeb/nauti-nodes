# nauti-nodes (Tier 3: Federated Compute Coordinator)
- **Role**: "Amway-Style" distributed authority and compute lease coordinator.
- **Operates**: Signed federation commands (`/internal/federation/command`), multi-node leases, worker fences, cluster sync.
- **Boundaries**: Infrastructure management only. Contains no SAR detection math or image filters.
