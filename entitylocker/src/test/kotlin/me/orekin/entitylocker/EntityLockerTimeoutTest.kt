package me.orekin.entitylocker

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

internal class EntityLockerTimeoutTest {

    @Test
    fun testSingleThreadExecutedIfTimeoutSpecified() {
        val locker = EntityLocker<Int>()
        val threadResult = AtomicBoolean(false)

        val attemptResult = locker.tryExecuteLocked(1, 100, TimeUnit.MILLISECONDS) {
            threadResult.set(true)
        }

        assertTrue(attemptResult)
        assertTrue(threadResult.get())
    }

    @Test
    fun testWaitingTimesOutAfterSpecifiedPeriod() {
        val locker = EntityLocker<Int>()
        val firstThreadEnterLatch = CountDownLatch(1)
        val firstThreadAttemptResult = AtomicBoolean(false)
        val secondThreadExitLatch = CountDownLatch(1)

        val firstThread = Thread {
            firstThreadEnterLatch.await()
            val attemptResult = locker.tryExecuteLocked(1, 100, TimeUnit.MILLISECONDS) {}
            firstThreadAttemptResult.set(attemptResult)
        }

        val secondThread = Thread {
            locker.executeLocked(1) {
                firstThreadEnterLatch.countDown()
                secondThreadExitLatch.await()
            }
        }

        firstThread.start()
        secondThread.start()

        Thread.sleep(200)

        secondThreadExitLatch.countDown()

        firstThread.join()
        secondThread.join()

        assertFalse(firstThreadAttemptResult.get())
    }

}
