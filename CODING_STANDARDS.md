# Coding standards

1. Keep simple, single-use logic inline. Do not extract a function only to make the parent shorter.
2. No trivial single-use helpers that make the reader jump between definitions for straightforward control flow.
3. Prefer readable linear code when the operations are tightly coupled.
4. Simplest implementation that works, but never at the cost of reliability. No speculative options, hooks or abstractions.
5. `src/ray_vast/worker/` uses the Python stdlib only (it runs inside user images that have only Python + Ray).
6. Secrets (`instance_api_key`, `CONTAINER_API_KEY`, join tokens, the cluster token, private keys, the vast key) never appear in logs, exception messages, the container command (`args`) or stdout. The only secret in the create request env is the join token (design.md 5.2).
7. Every vast and AWS call has a timeout. vast calls retry on HTTP 429; reads and destroys also retry on 5xx and connection errors; a create retries only on 429 (a retry could rent twice).
8. The provider holds one `RLock`. No network call while the lock is held.
9. Every path that ends a candidate or a worker, in the provider and in the head server, calls `VastNodeProvider._end`. It destroys the vast instance, updates the IP rule ref count and removes the rule at 0. No other code destroys a known instance; the sweeps (`vast_api.sweep`) destroy only instances that are not in the state.
10. Tests test behavior at public seams. Mock only system boundaries (vast HTTP API, AWS, clock, randomness).
