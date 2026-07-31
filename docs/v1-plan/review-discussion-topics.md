# V1 plan review discussion topics

- [ ] The best way to do “pinned hermetic-Python semantics” design.
- [ ] Budgets default to not set rule, versus explicitly setting budgets for no
      reason.
- [ ] Marked truncation: I can't really imagine a setting where this is what the
      end user would want, like, I'd want the beginning and the end and then to
      drop some stuff in the middle, I'd never want just the beginning??
- [ ] The rule is, we always default to recording, then why is an “extension”
      that we default to not recording and only record when it is selected??
- [ ] For the batch setup, are we building this to be able to take advantage of
      many cores and many processes per core on a given machine (and/or threads,
      I'm never exactly sure which combo of these three we want in order to max
      out machine usage)?
- [ ] Does it make sense to output JSON when we could instead use a more standard
      ML format like NumPy arrays/tensors or Parquet?
- [ ] It would be nice to have a way to limit read/write to the filesystem and
      network interactions in this v1, especially if we can do it in a way that
      isn't expensive in terms of setup and teardown.

## Contract contradiction notes

- [ ] **Contradiction 1 — budgets come from declared meaning:** Update the core
      contract to make clear that RAM protection is an optional default, not an
      expectation. Unbudgeted and RAM-protective defaults are both valid
      choices.
- [ ] **Contradiction 2 — call-scoped lifecycle:** Address the process-tree
      teardown contradiction in the v1 design.
- [ ] **Contradiction 3 — durable observability:** Think through a core-contract
      distinction in which test runs do not need durable and faithful
      observability, real runs do, and the real-run guarantee is tested.
