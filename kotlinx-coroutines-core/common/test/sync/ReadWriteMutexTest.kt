/*
 * Copyright 2016-2020 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.sync

import kotlinx.coroutines.*
import kotlin.test.*

@OptIn(HazardousConcurrentApi::class)
class ReadWriteMutexTest : TestBase() {
    @Test
    fun `simple single coroutine test`() = runTest {
        val m = ReadWriteMutex()
        m.readLock()
        m.readLock()
        m.readUnlock()
        m.readUnlock()
        m.writeLock()
        m.writeUnlock()
        m.readLock()
    }

    @Test
    fun `simple multiple coroutines test`() = runTest {
        val m = ReadWriteMutex()
        m.readLock()
        expect(1)
        launch {
            expect(2)
            m.readLock()
            expect(3)
        }
        yield()
        expect(4)
        launch {
            expect(5)
            m.writeLock()
            expect(8)
        }
        yield()
        expect(6)
        m.readUnlock()
        yield()
        expect(7)
        m.readUnlock()
        yield()
        finish(9)
    }

    @Test
    fun `acquireRead does not suspend after cancelled acquireWrite`() = runTest {
        val m = ReadWriteMutex()
        m.readLock()
        val wJob = launch {
            expect(1)
            m.writeLock()
            expectUnreached()
        }
        yield()
        expect(2)
        wJob.cancel()
        launch {
            expect(3)
            m.readLock()
            expect(4)
        }
        yield()
        finish(5)
    }
}