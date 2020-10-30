/*
 * Copyright 2016-2020 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.sync

import kotlinx.atomicfu.*
import kotlinx.coroutines.*
import kotlinx.coroutines.internal.SegmentQueueSynchronizer

/**
 * A readers-writer mutex maintains a logical pair of locks, one for
 * read-only operations, that can be processed concurrently, and one
 * for write operations which guarantees an exclusive access so that
 * neither write or read operation can be processed in parallel.
 *
 * Similarly to [Mutex], this readers-writer mutex is  **non-reentrant**,
 * that is invoking [readLock] or [writeLock] even from the same thread or
 * coroutine that currently holds the corresponding lock still suspends the invoker.
 * At the same time, invoking [readLock] from the holder of the write lock
 * also suspends the invoker.
 *
 * The typical usage of [ReadWriteMutex] is wrapping each read invocation with
 * [ReadWriteMutex.withReadLock] and each write invocation with [ReadWriteMutex.withWriteLock]
 * correspondingly. These wrapper functions guarantee that the mutex is used
 * correctly and safely. However, one can use `lock` and `unlock` operations directly,
 * but there is a contract that `unlock` should be invoked only after a successful
 * corresponding `lock` invocation. Since this low-level API is potentially error-prone,
 * it is marked as [HazardousConcurrentApi] and requires the corresponding [OptIn] declaration.
 *
 * The advantage of using [ReadWriteMutex] comparing to plain [Mutex] is an
 * availability to parallelize read operations and, therefore, increasing the
 * level of concurrency. It is extremely useful for the workloads with dominating
 * read operations so that they can be executed in parallel and improve the
 * performance. However, depending on the updates frequence, the execution cost of
 * read and write operations, and the contention, it can be cheaper to use a plain [Mutex].
 * Therefore, it is highly recommended to measure the performance difference
 * to make a right choice.
 */
public interface ReadWriteMutex {
    /**
     * Acquires a read lock of this mutex if the write lock is not acquired,
     * suspends the caller otherwise until the write lock is released. TODO fairness
     *
     * This suspending function is cancellable. If the [Job] of the current coroutine is cancelled or completed while this
     * function is suspended, this function immediately resumes with [CancellationException].
     * There is a **prompt cancellation guarantee**. If the job was cancelled while this function was
     * suspended, it will not resume successfully. See [suspendCancellableCoroutine] documentation for low-level details.
     * This function releases the lock if it was already acquired by this function before the [CancellationException]
     * was thrown.
     *
     * Note that this function does not check for cancellation when it is not suspended.
     * Use [yield] or [CoroutineScope.isActive] to periodically check for cancellation in tight loops if needed.
     *
     * TODO HazardousConcurrentApi
     */
    @HazardousConcurrentApi
    public suspend fun readLock()

    /**
     * Releases a read lock of this mutex and resumes the first waiting writer
     * if there is the one and this operation releases the last read lock.
     *
     * TODO HazardousConcurrentApi
     */
    @HazardousConcurrentApi
    public fun readUnlock()

    /**
     * TODO
     */
    @HazardousConcurrentApi
    public suspend fun writeLock()

    /**
     * TODO
     */
    @HazardousConcurrentApi
    public fun writeUnlock()
}

/**
 * Creates a new [ReadWriteMutex] instance,
 * both read and write locks are not acquired.
 *
 * TODO: fairness
 */
public fun ReadWriteMutex(): ReadWriteMutex = ReadWriteMutexImpl()

/**
 * Executes the given [action] under a _read_ mutex's lock.
 *
 * @return the return value of the [action].
 */
@OptIn(HazardousConcurrentApi::class)
public suspend inline fun <T> ReadWriteMutex.withReadLock(action: () -> T): T {
    readLock()
    try {
       return action()
    } finally {
        readUnlock()
    }
}

/**
 * Executes the given [action] under the _write_ mutex's lock.
 *
 * @return the return value of the [action].
 */
@OptIn(HazardousConcurrentApi::class)
public suspend inline fun <T> ReadWriteMutex.withWriteLock(action: () -> T): T {
    writeLock()
    try {
        return action()
    } finally {
        writeUnlock()
    }
}

/**
 * This readers-writer mutex maintains two atomic variables [R] and [W], and uses two
 * separate [SegmentQueueSynchronizer]-s for waiting readers and writers. The 64-bit
 * variable [R] maintains three mostly readers-related states atomically:
 * - `AWF` (active writer flag) bit that is `true` if there is a writer holding the write lock.
 * - `WWF` (waiting writer flag) bit that is `true` if there is a writer waiting for the write lock
 *                               and the lock is not acquired due to active readers.
 * - `AR` (active readers) 30-bit counter which represents the number of coroutines holding a read lock,
 * - `WR` (waiting readers) 30-bit counter which represents the number of coroutines waiting for a
 *                          read lock in the corresponding [SegmentQueueSynchronizer].
 * This way, when a reader comes for a lock, it atomically checks whether the `WF` flag is set and
 * either increments the `AR` counter and acquires a lock if it is not set, or increments the
 * `WR` counter and suspends otherwise. At the same time, when a reader releases the lock, it
 * it checks whether it is the last active reader and resumes the first  waiting writer if the `WF`
 * flag is set.
 *
 * Writers, on their side, use an additional [W] field which represents the number of waiting
 * writers as well as several internal flags:
 * - `WW` (waiting writers) 30-bit counter that represents the number of coroutines  waiting for
 *                          the write lock in the corresponding [SegmentQueueSynchronizer],
 * - `WLA` (the write lock is acquired) flag which is `true` when the write lock is acquired,
 *                                      and `WF` should be `true` as well in this case,
 * - `WLRP` (write lock release is in progress) flag which is `true` when the [releaseWrite]
 *                                              invocation is in progress. Note, that `WLA`
 *                                              should already be `false` in this case.
 * - `WRF` (writer is resumed) flag that can be set to `true` by [releaseRead] if there is a
 *                             concurrent [releaseWrite], so that `WLRP` is set to true. This
 *                             flag helps to manage the race when [releaseWrite] successfully
 *                             resumed waiting readers, has not re-set `WF` flag in [R] yet,
 *                             while there readers completed with [releaseRead] and the last
 *                             one observes the `WF` flag set to `true`, so that it should try
 *                             to resume the next waiting writer. However, it is better to tell
 *                             the concurrent [releaseWrite] to check whether there is a writer
 *                             and resume it.
 *
 */
private class ReadWriteMutexImpl : ReadWriteMutex {
    private val R = atomic(0L)
    private val W = atomic(0)

    private val sqsForWriters = object : SegmentQueueSynchronizer<Unit>() {
        override val resumeMode: ResumeMode get() = ResumeMode.ASYNC
        override val cancellationMode: CancellationMode get() = CancellationMode.SMART_ASYNC

        override fun onCancellation(): Boolean {
            // Decrement the number of waiting writers
            while (true) {
                val w = curW
                if (w.ww == 0) return false
                if (w.ww > 1 || w.wla || w.wlrp) {
                    if (casW(w, constructW(w.ww - 1, w.wla, w.wlrp, w.wrf))) return true
                } else {
                    // w.ww == 1, w.wla == false, w.wlrp == false, w.wrf == false
                    if (casW(w, constructW(1, false, true, false))) break
                }
            }
            // The WLRP flag is successfully set.
            // Re-set the WWF flag or complete if the AWF flag is set.
            while (true) {
                val r = R.value
                val awf = r.awf
                val wwf = r.wwf
                val ar = r.ar
                val wr = r.wr
                assert { ar > 0 && wwf || awf } // Either active writer or active reader should be here
                // Should this last cancelled waiting writer be resumed by the last reader?
                if (awf) {
                    // Re-set the WLRP flag
                    while (true) {
                        val w = W.value
                        val ww = w.ww
                        val wla = w.wla
                        val wlrp = w.wlrp
                        val wrf = w.wrf
                        assert { wlrp } // WLRP flag should still be set
                        assert { !wla } // WLA cannot be set here
                        if (wrf) {
                            // Are we the last waiting writer?
                            if (ww == 1) {
                                val updW = constructW(0, true, false, false)
                                if (W.compareAndSet(w, updW)) return false
                            } else {
                                val updW = constructW(ww - 2, true, false, false)
                                if (W.compareAndSet(w, updW)) {
                                    resume(Unit)
                                    return true
                                }
                            }
                        } else {
                            assert { ww > 0 } // Our cancelling writer should still be counted
                            val updW = constructW(ww, false, false, false)
                            if (W.compareAndSet(w, updW)) return true
                        }
                    }
                }
                // Try to reset the WWF flag and resume waiting readers.
                if (R.compareAndSet(r, constructR(false, false, ar + wr, 0))) {
                    // Were there waiting readers?
                    // Resume them physically if needed.
                    repeat(wr) {
                        val success = sqsForReaders.resume(Unit)
                        assert { success } // Reader resumption cannot fail because of the smart cancellation in the SQS
                    }
                    break
                }
            }
            // Check whether the AWF or WWF flag should be set back due to new waiting writers.
            // Phase 3. Try to re-set the WLRP flag
            // if there is no waiting writer.
            while (true) {
                val w = W.value
                val ww = w.ww
                val wla = w.wla
                val wlrp = w.wlrp
                val wrf = w.wrf
                assert{ wlrp } // WLRP flag should still be set
                assert{ !wla } // There should be no active writer at this point: $this
                assert{ !wrf } // WRF flag can be set only when the WF flag is put back
                // Is there a waiting writer?
                if (ww > 1) break
                // No waiting writers, try
                // to complete immediately.
                val updW = constructW(0, false, false, false)
                if (W.compareAndSet(w, updW)) return true
            }
            // Phase 4. There is a waiting writer,
            // set the WF flag back and try to grab
            // the write lock.
            var acquired: Boolean
            while (true) {
                val r = R.value
                val awf = r.awf
                val wwf = r.wwf
                val ar = r.ar
                val wr = r.wr
                assert{ !awf && !wwf } // The WF flag should not be set at this point
                assert{ wr == 0 } // The number of waiting readers shold be 0 when the WF flag is not set
                acquired = ar == 0
                val updR = constructR(acquired, !acquired, ar, 0)
                // Try to set the WF flag.
                if (R.compareAndSet(r, updR)) break
            }
            // Phase 5. Re-set the WLRP flag and try to resume
            // the next waiting writer if the write lock is grabbed.
            while (true) {
                val w = W.value
                val ww = w.ww
                val wla = w.wla
                val wlrp = w.wlrp
                val wrf = w.wrf
                assert{ ww > 0 } // WW cannot decrease without resumptions
                assert{ wlrp } // WLRP flag should still be set
                assert{ !wla } // WLA cannot be set here
                val resume = acquired || wrf
                val updW = constructW(if (resume) ww - 1 else ww, resume, false, false)
                if (!W.compareAndSet(w, updW)) continue
                if (resume) resume(Unit)
                return true
            }
        }

        @OptIn(HazardousConcurrentApi::class)
        override fun tryReturnRefusedValue(value: Unit): Boolean {
            writeUnlock()
            return true
        }
    }

    private val curR get() = R.value
    private val curW get() = W.value
    private fun casR(cur: Long, new: Long) = R.compareAndSet(cur, new)
    private fun casW(cur: Int, new: Int) = W.compareAndSet(cur, new)

    private val sqsForReaders = object : SegmentQueueSynchronizer<Unit>() {
        override val resumeMode: ResumeMode get() = ResumeMode.ASYNC
        override val cancellationMode: CancellationMode get() = CancellationMode.SMART_ASYNC

        override fun onCancellation(): Boolean {
            while (true) {
                val r = curR
                if (r.wr == 0) return false
                if (casR(r, constructR(r.awf, r.wwf, r.ar, r.wr - 1))) {
                    return true
                }
            }
        }

        @OptIn(HazardousConcurrentApi::class)
        override fun tryReturnRefusedValue(value: Unit): Boolean {
            readUnlock()
            return true
        }
    }

    @HazardousConcurrentApi
    override suspend fun readLock(): Unit = R.loop { r ->
        // Read the current [R] state.
        val awf = r.awf
        val wwf = r.wwf
        val ar = r.ar
        val wr = r.wr
        // Is there an active or waiting writer?
        if (!awf && !wwf) {
            // There is no writer, try to grab a read lock!
            assert { wr == 0 }
            val upd = constructR(false, false, ar + 1, 0)
            if (R.compareAndSet(r, upd)) return
        } else {
            // This reader should wait for a lock, try to
            // increment the number of waiting readers and suspend.
            val upd = constructR(awf, wwf, ar, wr + 1)
            if (R.compareAndSet(r, upd)) {
                suspendCancellableCoroutine<Unit> { sqsForReaders.suspend(it) }
                return
            }
        }
    }

    @HazardousConcurrentApi
    override suspend fun writeLock() {
        try_again@while (true) {
            // Increment the number of waiting writers at first.
            val w = W.getAndIncrement()
            val ww = w.ww
            val wla = w.wla
            val wlrp = w.wlrp
            // Is this writer the first one? Check whether there are other
            // writers waiting for the lock, an active one, or a concurrent
            // [releaseWrite] invocation is in progress.
            if (wla || wlrp) {
                // Try to suspend and re-try the whole operation on failure.
                suspendCancellableCoroutine<Unit> { cont ->
                    sqsForWriters.suspend(cont)
                }
                return
            }
            // This writer is the first one. Set the `WF` flag and
            // complete immediately if there is no reader.
            while (true) {
                val r = R.value
                val awf = r.awf
                val wwf = r.wwf
                val ar = r.ar
                val wr = r.wr
                if (awf || wwf) {
                    // Try to suspend and re-try the whole operation on failure.
                    suspendCancellableCoroutine<Unit> { cont ->
                        sqsForWriters.suspend(cont)
                    }
                    return
                }
                assert{ wr == 0 } // The number of waiting readers should be 0 when the WF flag is not set
                val acquired = ar == 0
                val rUpd = constructR(acquired, !acquired, ar, wr)
                if (R.compareAndSet(r, rUpd)) {
                    // Is the lock acquired? Check the number of readers.
                    if (acquired) {
                        // Yes! The write lock is just acquired!
                        // Update `W` correspondingly.
                        W.update { w1 ->
                            val ww1 = w1.ww
                            val wla1 = w1.wla
                            val wlrp1 = w1.wlrp
                            val wrf1 = w1.wrf
                            assert{ ww1 > 0 } // WW should be greater than 0 at least because of this `acquireWrite` invocation
                            assert{ !wla1 && !wlrp1 && !wrf1 } // WLA, WLRP, and WRF flags should not be set here
                            constructW(ww1 - 1, true, false, false)
                        }
                        return
                    }
                    // There were active readers at the point of `WF` flag placing.
                    // Try to suspend and re-try the whole operation on failure.
                    // Note, that the thread that fails on resumption should
                    // re-set the WF flag if required.
                    suspendCancellableCoroutine<Unit> { cont ->
                        sqsForWriters.suspend(cont)
                    }
                    return
                }
            }
        }
    }

    @HazardousConcurrentApi
    override fun readUnlock() {
        while (true) {
            // Read the current [R] state
            val r = R.value
            val wwf = r.wwf
            val ra = r.ar
            val wr = r.wr
            assert{ ra > 0 } // No active reader to release
            assert{ !r.awf } // Write lock cannot be acquired when there is an active reader
            // Try to decrement the number of active readers
            val awfUpd = wwf && ra == 1
            val wwfUpd = wwf && !awfUpd
            val upd = constructR(awfUpd, wwfUpd, ra - 1, wr)
            if (!R.compareAndSet(r, upd)) continue
            // Check whether the current reader is the last one,
            // and resume the first writer if the `WF` flag is set.
            if (!awfUpd) return
            while (true) {
                // Either WLA, WLA or WLRP should be
                // non-zero when the WF flag is set.
                val w = W.value
                val ww = w.ww
                val wla = w.wla
                val wlrp = w.wlrp
                val wrf = w.wrf
                assert{ !wla } // There should be no active writer at this point
                assert{ !wrf } // The WRF flag cannot be set at this point
                // Is there still a concurrent [releaseWrite]?
                // Try to delegate the resumption work in this case.
                if (wlrp) {
                    val updW = constructW(ww, wla, true, true)
                    if (W.compareAndSet(w, updW)) return
                } else {
                    if (ww == 0) return // cancellation is happening TODO
                    assert{ ww > 0 } // At most one waiting writer should be registered at this point
                    // Try to set the `WLA` flag and decrement
                    // the number of waiting writers.
                    val updW = constructW(ww - 1, true, false, false)
                    if (!W.compareAndSet(w, updW)) continue
                    // Try to resume the first waiting writer.
                    sqsForWriters.resume(Unit)
                    return
                }
            }
        }
    }

    @HazardousConcurrentApi
    override fun writeUnlock() {
        // Phase 1. Try to resume the next waiting writer
        // or re-set the WLA flag and set the WLRP one.
        while (true) {
            val w = W.value
            val ww = w.ww
            val wla = w.wla
            val wlrp = w.wlrp
            val wrf = w.wrf
            assert{ wla } // Write lock is not acquired
            assert{ !wlrp && !wrf } // WLRP and WRF flags should not be set in the beginning
            // Is there a waiting writer?
            if (ww > 0) {
                val updW = constructW(ww - 1, true, false, false)
                if (W.compareAndSet(w, updW) && sqsForWriters.resume(Unit)) return
            } else {
                assert{ ww == 0 } // WW can be negative
                // Re-set the WLA flag and set the WLRP one.
                val updW = constructW(ww, false, true, false)
                if (W.compareAndSet(w, updW)) break
            }
        }
        // Phase 2. Re-set the WF flag
        // and resume readers if needed.
        while (true) {
            val r = R.value
            val awf = r.awf
            val wwf = r.wwf
            val ar = r.ar
            val wr = r.wr
            assert{ ar == 0 } // There should be no active reader while the write lock is acquired
            assert{ awf } // AWF should be set here
            assert{ !wwf } // WWF should not be set here
            // Re-set the WF flag and resume the waiting readers logically.
            if (R.compareAndSet(r, constructR(false, false, wr, 0))) {
                // Were there waiting readers?
                // Resume them physically if needed.
                repeat(wr) {
                    val success = sqsForReaders.resume(Unit)
                    assert{ success } // Reader resumption cannot fail because of the smart cancellation in the SQS
                }
                break
            }
        }
        // Phase 3. Try to re-set the WLRP flag
        // if there is no waiting writer.
        while (true) {
            val w = W.value
            val ww = w.ww
            val wla = w.wla
            val wlrp = w.wlrp
            val wrf = w.wrf
            assert{ wlrp } // WLRP flag should still be set
            assert{ !wla } // There should be no active writer at this point: $this
            assert{ !wrf } // WRF flag can be set only when the WF flag is put back
            // Is there a waiting writer?
            if (ww != 0) break
            // No waiting writers, try
            // to complete immediately.
            val updW = constructW(0, false, false, false)
            if (W.compareAndSet(w, updW)) return
        }
        // Phase 4. There is a waiting writer,
        // set the WF flag back and try to grab
        // the write lock.
        var acquired: Boolean
        while (true) {
            val r = R.value
            val awf = r.awf
            val wwf = r.wwf
            val ar = r.ar
            val wr = r.wr
            assert{ !awf && !wwf } // The WF flag should not be set at this point
            assert{ wr == 0 } // The number of waiting readers shold be 0 when the WF flag is not set
            acquired = ar == 0
            val updR = constructR(acquired, !acquired, ar, 0)
            // Try to set the WF flag.
            if (R.compareAndSet(r, updR)) break
        }
        // Phase 5. Re-set the WLRP flag and try to resume
        // the next waiting writer if the write lock is grabbed.
        while (true) {
            val w = W.value
            val ww = w.ww
            val wla = w.wla
            val wlrp = w.wlrp
            val wrf = w.wrf
            assert{ wlrp } // WLRP flag should still be set
            assert{ !wla } // WLA cannot be set here
            val resume = acquired || wrf
            if (resume && ww == 0) {
                val updW = constructW(0, true, false, false)
                if (!W.compareAndSet(w, updW)) continue
                writeUnlock()
                return
            } else {
                val updW = constructW(if (resume) ww - 1 else ww, resume, false, false)
                if (!W.compareAndSet(w, updW)) continue
                if (resume) sqsForWriters.resume(Unit)
                return
            }
        }
    }

    override fun toString() = "R=<${R.value.awf},${R.value.wwf},${R.value.ar},${R.value.wr}>," +
        "W=<${W.value.ww},${W.value.wla},${W.value.wlrp},${W.value.wrf}>"

    companion object {
        inline val Long.awf: Boolean get() = this and AWF_BIT != 0L
        const val AWF_BIT = 1L shl 62

        inline val Long.wwf: Boolean get() = this and WWF_BIT != 0L
        const val WWF_BIT = 1L shl 61

        inline val Long.wr: Int get() = (this and WAITERS_MASK).toInt()
        const val WAITERS_MASK = (1L shl 30) - 1L

        inline val Long.ar: Int get() = ((this and ACTIVE_MASK) shr 30).toInt()
        const val ACTIVE_MASK = (1L shl 60) - 1L - WAITERS_MASK

        @Suppress("NOTHING_TO_INLINE")
        inline fun constructR(awf: Boolean, wwf: Boolean, ar: Int, wr: Int): Long {
            var res = 0L
            if (awf) res += AWF_BIT
            if (wwf) res += WWF_BIT
            res += ar.toLong() shl 30
            res += wr.toLong()
            return res
        }

        const val WRITER_ACTIVE_FLAG: Int = 10_000_000
        const val RELEASE_IN_PROGRESS_FLAG = 100_000_000
        const val WRITER_RESUMED_FLAG = 1_000_000_000

        inline val Int.ww : Int get() = this % WRITER_ACTIVE_FLAG
        inline val Int.wla : Boolean get() = (this / WRITER_ACTIVE_FLAG) % 10 == 1
        inline val Int.wlrp : Boolean get() = (this / RELEASE_IN_PROGRESS_FLAG) % 10 == 1
        inline val Int.wrf : Boolean get() = (this / WRITER_RESUMED_FLAG) % 10 == 1

        inline fun constructW(ww: Int, wla: Boolean, wlrp: Boolean, wrf: Boolean): Int {
            var res = ww
            if (wla) res += WRITER_ACTIVE_FLAG
            if (wlrp) res += RELEASE_IN_PROGRESS_FLAG
            if (wrf) res += WRITER_RESUMED_FLAG
            return res
        }
    }
}