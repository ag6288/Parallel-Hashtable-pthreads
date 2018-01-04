# Parallel-Hashtable-pthreads
Parallel Hashtable with pthreads

- `insert` and `retrieve` do not lose items when run from multiple threads.
- Copy of `parallel_mutex.c` which is `parallel_spin.c` and replaced all of the mutex APIs with the spinlock APIs in pthreads.
- Multiple `retrieve` operations can run in parallel.
- `insert` operations can run in parallel.
