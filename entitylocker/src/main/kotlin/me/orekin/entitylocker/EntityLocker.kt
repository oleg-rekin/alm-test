package me.orekin.entitylocker

import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
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

    private val lockedEntityDetailsById = ConcurrentHashMap<ID, LockedEntityDetails>()

    private val waitingQueuesByEntityId = ConcurrentHashMap<ID, Queue<WaitingThreadDetails>>()

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
        val semaphoreToRelease = getNextLockedFromQueueIfExistsOrReleaseEntity(entityId, currentThreadId)
        semaphoreToRelease?.release()
    }

    private fun recheckIsEntityLockedAndAddToQueueIfNeeded(entityId: ID, currentThreadId: Long): Semaphore? {
        waitingQueueLock.acquire()
        try {
            if (tryLockAndStoreDetails(entityId, currentThreadId)) {
                // The entity has been released, so we do not need to wait
                return null
            }
            val lockedDetails = lockedEntityDetailsById[entityId]
                ?: // Should never happen
                throw IllegalMonitorStateException("Entity with ID '$entityId' is locked but there is no details on the lock. This EntityLocker instance's state is corrupted")

            if (lockedDetails.holdingThreadId == currentThreadId) {
                lockedDetails.acquisitionsCount.incrementAndGet()
                // The entity lock is held by the same thread, so we do not need to wait
                return null
            }

            val waitingQueue = waitingQueuesByEntityId.computeIfAbsent(entityId) { ConcurrentLinkedQueue() }
            val waitingThreadDetails = WaitingThreadDetails(currentThreadId, Semaphore(0))
            waitingQueue.add(waitingThreadDetails)
            return waitingThreadDetails.semaphoreToLockOn
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

    private fun getNextLockedFromQueueIfExistsOrReleaseEntity(entityId: ID, currentThreadId: Long): Semaphore? {
        waitingQueueLock.acquire()
        try {
            val lockedEntityDetails = lockedEntityDetailsById[entityId]
                ?: // Should never happen
                throw IllegalMonitorStateException("Entity with ID '$entityId' is locked but there is no details on the lock. This EntityLocker instance's state is corrupted")

            if (lockedEntityDetails.holdingThreadId != currentThreadId) {
                // Should never happen as `lock()`/`unlock()` methods are private
                throw IllegalMonitorStateException("Trying to release entity with ID '$entityId' locked by another thread")
            }

            if (lockedEntityDetails.acquisitionsCount.decrementAndGet() > 0) {
                // The lock should be released at least once more, nothing to do
                return null
            }

            val waitingQueue = waitingQueuesByEntityId[entityId]

            if (waitingQueue == null) {
                // Have to do it under the waitingQueueLock lock in case there is a concurrent lock request
                // that failed the fast track locking. As releasing (this line) is under the waitingQueueLock lock,
                // that request either have already added a waiting semaphore or will recheck the lockedEntityIds later.
                releaseEntityAndClearDetails(entityId)
                return null
            }

            val nextWaitingThread = waitingQueue.remove()
            if (nextWaitingThread == null) {
                // Should never happen (the queue should be removed upon taking the last element), TODO use proper logging
                println("Empty queue was not removed for entity ID $entityId, removing it now")
                waitingQueuesByEntityId.remove(entityId)

                // Same behavior as if there was no queue, see above
                releaseEntityAndClearDetails(entityId)
                return null
            }

            // There is a waiting thread
            if (waitingQueue.isEmpty()) {
                waitingQueuesByEntityId.remove(entityId)
            }
            lockedEntityDetailsById[entityId] = LockedEntityDetails(nextWaitingThread.threadId, AtomicLong(1))
            return nextWaitingThread.semaphoreToLockOn
        } finally {
            waitingQueueLock.release()
        }
    }

    private fun releaseEntityAndClearDetails(entityId: ID) {
        lockedEntityIds.remove(entityId)
        lockedEntityDetailsById.remove(entityId)
    }
}
