package me.orekin.entitylocker

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.Test
import kotlin.test.assertTrue

internal class EntityLockerReentrancyTest {

    @Test
    fun testSameThreadLocksTwice() {
        val locker = EntityLocker<Int>()
        val result = AtomicBoolean(false)

        locker.executeLocked(1) {
            locker.executeLocked(1) {
                result.set(true)
            }
        }

        assertTrue(result.get())
    }

    @Test
    fun testEntityFreeAfterMultipleLockingsBySameThread() {
        val locker = EntityLocker<Int>()
        val result = AtomicBoolean(false)

        val firstThread = Thread {
            locker.executeLocked(1) {
                locker.executeLocked(1) {}
            }
        }

        firstThread.start()
        firstThread.join()

        val secondThread = Thread {
            locker.executeLocked(1) {
                result.set(true)
            }
        }

        secondThread.start()
        secondThread.join()

        assertTrue(result.get())
    }

    @Test
    fun testWaitingThreadReleasedAfterMultipleLockingsBySameThread() {
        val locker = EntityLocker<Int>()
        val multiLockedThreadExitLatch = CountDownLatch(1)
        val secondThreadResult = AtomicBoolean(false)

        val firstThread = Thread {
            locker.executeLocked(1) {
                locker.executeLocked(1) {
                    multiLockedThreadExitLatch.await()
                }
            }
        }


        val secondThread = Thread {
            locker.executeLocked(1) {
                secondThreadResult.set(true)
            }
        }

        firstThread.start()

        // Waiting to be sure firstThread acquired the lock
        Thread.sleep(100)

        secondThread.start()

        // Waiting to be sure secondThread is locked
        Thread.sleep(100)

        multiLockedThreadExitLatch.countDown()

        firstThread.join()
        secondThread.join()

        assertTrue(secondThreadResult.get())
    }

}
