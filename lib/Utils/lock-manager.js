import { makeKeyedMutex } from './make-mutex.js';
const refKey = (r) => `${r.namespace}\0${r.id}`;
const compareRefs = (a, b) => {
    const ak = refKey(a);
    const bk = refKey(b);
    return ak < bk ? -1 : ak > bk ? 1 : 0;
};
/**
 * Build a {@link LockManager} on top of the existing {@link makeKeyedMutex}
 * primitive (which already implements correct identity-checked refcount
 * cleanup at `src/Utils/make-mutex.ts:34`). This module is the canonical
 * lock-acquisition surface for the auth/transaction layer — no other module
 * should instantiate per-key mutexes or `PQueue` maps directly.
 */
export const makeLockManager = () => {
    const km = makeKeyedMutex();
    // Track in-flight keys for `isLocked` (a thin counter, not load-bearing).
    const heldCounts = new Map();
    const noteHeld = (key) => heldCounts.set(key, (heldCounts.get(key) ?? 0) + 1);
    const noteReleased = (key) => {
        const next = (heldCounts.get(key) ?? 1) - 1;
        if (next <= 0)
            heldCounts.delete(key);
        else
            heldCounts.set(key, next);
    };
    const withSingleLock = async (ref, work) => {
        const key = refKey(ref);
        return km.mutex(key, async () => {
            noteHeld(key);
            try {
                return await work();
            }
            finally {
                noteReleased(key);
            }
        });
    };
    return {
        withLock: withSingleLock,
        withLocks(refs, work) {
            if (refs.length === 0)
                return Promise.resolve().then(work);
            // De-duplicate and sort to enforce a global acquisition order across
            // all callers — this is what prevents `withLocks([a, b])` from
            // deadlocking against `withLocks([b, a])`.
            const seen = new Set();
            const sorted = [...refs].sort(compareRefs).filter(r => {
                const k = refKey(r);
                if (seen.has(k))
                    return false;
                seen.add(k);
                return true;
            });
            // Acquire by recursive nesting — each level holds its own lock for the
            // duration of the inner acquisitions and the final `work()` call. This
            // is the natural shape over `runExclusive`-style mutexes.
            const acquire = (i) => {
                if (i === sorted.length)
                    return Promise.resolve().then(work);
                return withSingleLock(sorted[i], () => acquire(i + 1));
            };
            return acquire(0);
        },
        isLocked(ref) {
            return heldCounts.has(refKey(ref));
        }
    };
};
//# sourceMappingURL=lock-manager.js.map