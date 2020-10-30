/*
 * Copyright 2016-2020 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.linearizability

import kotlinx.coroutines.*
import kotlinx.coroutines.check
import kotlinx.coroutines.sync.*
import org.jetbrains.kotlinx.lincheck.*
import org.jetbrains.kotlinx.lincheck.annotations.*
import org.jetbrains.kotlinx.lincheck.annotations.Operation
import org.jetbrains.kotlinx.lincheck.paramgen.*
import org.jetbrains.kotlinx.lincheck.strategy.managed.modelchecking.*
import org.jetbrains.kotlinx.lincheck.strategy.stress.*
import org.jetbrains.kotlinx.lincheck.verifier.*
import org.junit.*

internal class ReadWriteMutexCounterLCStressTest {
    val m = ReadWriteMutex()
    var c = 0

    @Operation(cancellableOnSuspension = true, allowExtraSuspension = true)
    suspend fun inc(): Int = m.withWriteLock { c++ }

    @Operation(cancellableOnSuspension = true, allowExtraSuspension = true)
    suspend fun get(): Int = m.withReadLock { c }

    @StateRepresentation
    fun stateRepresentation(): String = "$c+$m"

    @Test
    fun test2() = ModelCheckingOptions()
        .iterations(50)
        .actorsBefore(0)
        .actorsAfter(0)
        .threads(2)
        .actorsPerThread(3)
        .logLevel(LoggingLevel.INFO)
        .invocationsPerIteration(100_000)
        .sequentialSpecification(ReadWriteMutexCounterSequential::class.java)
        .check(this::class)

    @Test
    fun test() = StressOptions()
        .actorsBefore(0)
        .actorsAfter(0)
        .threads(3)
        .actorsPerThread(5)
        .invocationsPerIteration(100_000)
        .logLevel(LoggingLevel.INFO)
        .sequentialSpecification(ReadWriteMutexCounterSequential::class.java)
        .check(this::class)
}

class ReadWriteMutexCounterSequential : VerifierState() {
    var c = 0
    suspend fun inc() = c++
    suspend fun get() = c

    override fun extractState() = c
}

@OptIn(HazardousConcurrentApi::class)
class ReadWriteMutexLCStressTest {
    private val m = ReadWriteMutex()
    private val readLockAcquired = IntArray(6)
    private val writeLockAcquired = BooleanArray(6)

    @Operation(cancellableOnSuspension = true, allowExtraSuspension = true)
    suspend fun readLock(@Param(gen = ThreadIdGen::class) threadId: Int) {
        m.readLock()
        readLockAcquired[threadId]++
    }

    @Operation
    fun readUnlock(@Param(gen = ThreadIdGen::class) threadId: Int): Boolean {
        if (readLockAcquired[threadId] == 0) return false
        m.readUnlock()
        readLockAcquired[threadId]--
        return true
    }

    @Operation(cancellableOnSuspension = true, allowExtraSuspension = true)
    suspend fun writeLock(@Param(gen = ThreadIdGen::class) threadId: Int) {
        m.writeLock()
        assert(!writeLockAcquired[threadId]) { "The mutex is not reentrant" }
        writeLockAcquired[threadId] = true
    }

    @Operation
    fun writeUnlock(@Param(gen = ThreadIdGen::class) threadId: Int): Boolean {
        if (!writeLockAcquired[threadId]) return false
        m.writeUnlock()
        writeLockAcquired[threadId] = false
        return true
    }

    @Test
    fun test() = LCStressOptionsDefault()
        .actorsBefore(0)
        .actorsAfter(0)
        .threads(3)
        .actorsPerThread(5)
        .invocationsPerIteration(500_000)
        .logLevel(LoggingLevel.INFO)
        .sequentialSpecification(ReadWriteMutexSequential::class.java)
        .check(this::class)

    @Test
    fun test2() = ModelCheckingOptions()
        .iterations(50)
        .actorsBefore(0)
        .actorsAfter(0)
        .threads(2)
        .actorsPerThread(3)
        .logLevel(LoggingLevel.INFO)
        .invocationsPerIteration(100_000)
        .sequentialSpecification(ReadWriteMutexSequential::class.java)
        .check(this::class)

    @StateRepresentation
    fun stateRepresentation() = m.toString()
}

class ReadWriteMutexSequential : VerifierState() {
    private var activeReaders = 0
    private var writeLockedOrWaiting = false
    private val waitingReaders = ArrayList<Pair<CancellableContinuation<Unit>, Int>>()
    private val waitingWriters = ArrayList<Pair<CancellableContinuation<Unit>, Int>>()
    // Thread-local info
    private val readLockAcquired = IntArray(6)
    private val writeLockAcquired = BooleanArray(6)

    suspend fun readLock(threadId: Int) {
        if (writeLockedOrWaiting) {
            suspendCancellableCoroutine<Unit> { cont ->
                val contWithThreadId = cont to threadId
                waitingReaders.add(contWithThreadId)
                cont.invokeOnCancellation {
                    waitingReaders.remove(contWithThreadId)
                }
            }
        } else {
            activeReaders++
        }
        readLockAcquired[threadId]++
    }

    fun readUnlock(threadId: Int): Boolean {
        if (readLockAcquired[threadId] == 0) return false
        readLockAcquired[threadId]--
        activeReaders--
        if (activeReaders == 0 && writeLockedOrWaiting) {
            while (waitingWriters.isNotEmpty()) {
                val (w, t) = waitingWriters.removeAt(0)
                if (w.tryResume0(Unit, { writeUnlock(t) })) return true
            }
            writeLockedOrWaiting = false
            val resumedReaders = waitingReaders.map { (r, t) ->
                r.tryResume0(Unit, { readUnlock(t) })
            }.filter { it }.count()
            waitingReaders.clear()
            activeReaders = resumedReaders
            return true
        }
        return true
    }

    suspend fun writeLock(threadId: Int) {
        if (activeReaders > 0 || writeLockedOrWaiting) {
            writeLockedOrWaiting = true
            suspendCancellableCoroutine<Unit> { cont ->
                val contWithThreadId = cont to threadId
                waitingWriters.add(contWithThreadId)
                cont.invokeOnCancellation {
                    waitingWriters.remove(contWithThreadId)
                    if (waitingWriters.isEmpty() && writeLockAcquired.all { !it }) {
                        writeLockedOrWaiting = false
                        activeReaders += waitingReaders.size
                        waitingReaders.forEach { (r, t) ->
                            r.tryResume0(Unit, { readUnlock(t) })
                        }
                        waitingReaders.clear()
                    }
                }
            }
        } else {
            writeLockedOrWaiting = true
        }
        writeLockAcquired[threadId] = true
    }

    fun writeUnlock(threadId: Int): Boolean {
        if (!writeLockAcquired[threadId]) return false
        writeLockAcquired[threadId] = false
        while (waitingWriters.isNotEmpty()) {
            val (w, t) = waitingWriters.removeAt(0)
            if (w.tryResume0(Unit, { writeUnlock(t) })) return true
        }
        writeLockedOrWaiting = false
        val resumedReaders = waitingReaders.map { (r, t) ->
            r.tryResume0(Unit, { readUnlock(t) })
        }.filter { it }.count()
        waitingReaders.clear()
        activeReaders = resumedReaders
        return true
    }

    override fun extractState() = "$activeReaders, $writeLockedOrWaiting, ${readLockAcquired.contentToString()}, ${writeLockAcquired.contentToString()}"
}