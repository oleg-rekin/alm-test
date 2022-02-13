package me.orekin.entitylocker

import java.util.Queue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Semaphore
import java.util.concurrent.locks.ReentrantLock

/**
 * [EntityLocker] is a reusable utility class that provides synchronization mechanism similar to row-level DB locking.
 * See tests for usage examples.
 *
 * Note that [ID] is expected to implement [Any.equals] and [Any.hashCode] in a meaningful way.
 */
class EntityLocker<ID : Any> {

    private val lockedEntityIds = ConcurrentHashMap.newKeySet<ID>()

    private val waitingQueuesByEntityId = ConcurrentHashMap<ID, Queue<Semaphore>>()

    private val waitingQueueLock = ReentrantLock()

    fun <R> executeLocked(entityId: ID, protectedBlock: () -> R): R {
        lock(entityId)
        try {
            return protectedBlock()
        } finally {
            unlock(entityId)
        }
    }

    private fun lock(entityId: ID) {
        if (lockedEntityIds.add(entityId)) {
            return
        }
        val semaphoreToLockOn = recheckIsEntityLockedAndAddToQueueIfNeeded(entityId)

        // Have to lock on the semaphore after releasing `waitingQueueLock`,
        // so this line cannot be moved into the method called above
        semaphoreToLockOn?.acquire()
    }

    private fun unlock(entityId: ID) {
        val semaphoreToFree = getNextLockedFromQueueIfExistsOrReleaseEntity(entityId)
        semaphoreToFree?.release()
    }

    private fun recheckIsEntityLockedAndAddToQueueIfNeeded(entityId: ID): Semaphore? {
        waitingQueueLock.lock()
        try {
            if (lockedEntityIds.add(entityId)) {
                // The entity has been released, so we do not need to wait
                return null
            }
            val waitingQueue = waitingQueuesByEntityId.computeIfAbsent(entityId) { ConcurrentLinkedQueue() }
            val lockedSemaphore = Semaphore(0)
            waitingQueue.add(lockedSemaphore)
            return lockedSemaphore
        } finally {
            waitingQueueLock.unlock()
        }
    }

    private fun getNextLockedFromQueueIfExistsOrReleaseEntity(entityId: ID): Semaphore? {
        waitingQueueLock.lock()
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
            waitingQueueLock.unlock()
        }
    }
}
