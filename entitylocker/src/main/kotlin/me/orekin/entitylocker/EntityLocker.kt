package me.orekin.entitylocker

import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicLong

/**
 * [EntityLocker] is a reusable utility class that provides synchronization mechanism similar to row-level DB locking.
 * See tests for usage examples.
 *
 * Note that [ID] is expected to implement [Any.equals] and [Any.hashCode] in a meaningful way.
 */
class EntityLocker<ID : Any> {

    private val lockedEntityIds = ConcurrentHashMap.newKeySet<ID>()

    private val lockedEntityDetailsById = HashMap<ID, LockedEntityDetails>()

    private val waitingQueuesByEntityId = HashMap<ID, Queue<Semaphore>>()

    private val waitingQueueLock = Semaphore(1)

    fun <R> executeLocked(entityId: ID, protectedBlock: () -> R): R {
        val currentThreadId = Thread.currentThread().id
        lock(entityId, currentThreadId)
        try {
            return protectedBlock()
        } finally {
            unlock(entityId, currentThreadId)
        }
    }

    private fun lock(entityId: ID, currentThreadId: Long) {
        if (tryLockAndStoreDetails(entityId, currentThreadId)) {
            return
        }
        val semaphoreToLockOn = recheckIsEntityLockedAndAddToQueueIfNeeded(entityId, currentThreadId)

        // Have to lock on the semaphore after releasing `waitingQueueLock`,
        // so this line cannot be moved into the method called above
        semaphoreToLockOn?.acquire()
    }

    private fun unlock(entityId: ID, currentThreadId: Long) {
        val semaphoreToFree = getNextLockedFromQueueIfExistsOrReleaseEntity(entityId)
        semaphoreToFree?.release()
    }

    private fun recheckIsEntityLockedAndAddToQueueIfNeeded(entityId: ID, currentThreadId: Long): Semaphore? {
        waitingQueueLock.acquire()
        try {
            if (tryLockAndStoreDetails(entityId, currentThreadId)) {
                // The entity has been released, so we do not need to wait
                return null
            }
            val lockedDetails = lockedEntityDetailsById[entityId]!!
            if (lockedDetails.holdingThreadId == currentThreadId) {
                lockedDetails.acquisitionsCount.incrementAndGet()
                // The entity lock is held by the same thread, so we do not need to wait
                return null
            }

            val waitingQueue = waitingQueuesByEntityId.computeIfAbsent(entityId) { LinkedList() }
            val lockedSemaphore = Semaphore(0)
            waitingQueue.add(lockedSemaphore)
            return lockedSemaphore
        } finally {
            waitingQueueLock.release()
        }
    }

    /**
     * Details are stored only in case of successful locking.
     * @return `true` if locked successfully, `false` otherwise.
     */
    private fun tryLockAndStoreDetails(entityId: ID, currentThreadId: Long): Boolean {
        if (!lockedEntityIds.add(entityId)) {
            return false
        }
        lockedEntityDetailsById[entityId] = LockedEntityDetails(currentThreadId, AtomicLong(1))
        return true
    }

    private fun getNextLockedFromQueueIfExistsOrReleaseEntity(entityId: ID): Semaphore? {
        waitingQueueLock.acquire()
        try {
            val waitingQueue = waitingQueuesByEntityId[entityId]

            if (waitingQueue == null) {
                // Have to do it under the waitingQueueLock lock in case there is a concurrent lock request
                // that failed the fast track locking. As releasing (this line) is under the waitingQueueLock lock,
                // that request either have already added a waiting semaphore or will recheck the lockedEntityIds later.
                lockedEntityIds.remove(entityId)
                return null
            }

            if (waitingQueue.isEmpty()) {
                // Should never happen, TODO use proper logging
                println("Empty queue was not removed for entity ID $entityId, removing it now")
                waitingQueuesByEntityId.remove(entityId)

                // Same behavior as if there was no queue, see above
                lockedEntityIds.remove(entityId)
                return null
            }

            val waitingSemaphore = waitingQueue.remove()
            if (waitingQueue.isEmpty()) {
                waitingQueuesByEntityId.remove(entityId)
            }
            return waitingSemaphore
        } finally {
            waitingQueueLock.release()
        }
    }
}
