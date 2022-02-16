package me.orekin.entitylocker

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

internal class EntityLockerBasicTest {

    @Test
    fun testSingleThreadExecution() {
        val locker = EntityLocker<Int>()
        val result = AtomicBoolean(false)

        locker.executeLocked(1) {
            result.set(true)
        }

        assertTrue(result.get())
    }

    @Test
    fun testConcurrentExecutionForDifferentEntities() {
        val locker = EntityLocker<Int>()
        val threadsExitLatch = CountDownLatch(2)

        val firstThread = Thread {
            locker.executeLocked(1) {
                threadsExitLatch.countDown()
                threadsExitLatch.await()
            }
        }
        val secondThread = Thread {
            locker.executeLocked(2) {
                threadsExitLatch.countDown()
                threadsExitLatch.await()
            }
        }

        firstThread.start()
        secondThread.start()

        firstThread.join()
        secondThread.join()
    }

    @Test
    fun testLockingForSameEntity() {
        val locker = EntityLocker<String>()
        val threadsEnterLatch = CountDownLatch(2)
        val threadsExitLatch = CountDownLatch(2)

        val firstThread = Thread {
            threadsEnterLatch.countDown()
            locker.executeLocked("1") {
                threadsExitLatch.countDown()
                threadsExitLatch.await()
            }
        }
        val secondThread = Thread {
            threadsEnterLatch.countDown()
            locker.executeLocked("1") {
                threadsExitLatch.countDown()
                threadsExitLatch.await()
            }
        }

        firstThread.start()
        secondThread.start()

        threadsEnterLatch.await()

        // Waiting to be sure threads are actually locked
        Thread.sleep(100)
        assertEquals(1, threadsExitLatch.count)

        threadsExitLatch.countDown()

        firstThread.join()
        secondThread.join()
    }

    @Test
    fun testMultipleWaitingThreadsForSameEntityExecuted() {
        val threadsCount = 10
        val locker = EntityLocker<Int>()
        val executedCriticalSectionsCount = AtomicInteger(0)

        val threads = (1..threadsCount)
            .map {
                Thread {
                    locker.executeLocked(1) {
                        executedCriticalSectionsCount.incrementAndGet()
                    }
                }
            }

        threads.forEach { it.start() }

        threads.forEach { it.join() }
        assertEquals(threadsCount, executedCriticalSectionsCount.get())
    }

}
