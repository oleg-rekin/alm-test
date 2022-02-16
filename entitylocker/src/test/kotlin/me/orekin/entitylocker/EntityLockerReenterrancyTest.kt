package me.orekin.entitylocker

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.Test
import kotlin.test.assertTrue

internal class EntityLockerReenterrancyTest {

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

//    @Test
//    fun testEntityFreeAfterMultipleLockingsBySameThread() {
//        val locker = EntityLocker<Int>()
//        val threadsExitLatch = CountDownLatch(2)
//
//        val firstThread = Thread {
//            locker.executeLocked(1) {
//                locker.executeLocked(1) {
//                    result.set(true)
//                }
//            }
//            locker.executeLocked(1) {
//                threadsExitLatch.countDown()
//                threadsExitLatch.await()
//            }
//        }
//        val secondThread = Thread {
//            locker.executeLocked(1) {
//                threadsExitLatch.countDown()
//                threadsExitLatch.await()
//            }
//        }
//
//        firstThread.start()
//        secondThread.start()
//
//        firstThread.join()
//        secondThread.join()
//    }

}
