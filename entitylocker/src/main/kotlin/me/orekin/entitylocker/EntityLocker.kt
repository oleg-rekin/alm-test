package me.orekin.entitylocker

import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

/**
 * [EntityLocker] is a reusable utility class that provides synchronization mechanism similar to row-level DB locking.
 *
 *
 * The locking is reentrant: if current thread already holds the lock for an entity,
 * subsequent locking requests from that thread for that entity will never block.
 *
 *
 * The class supports locking with timeout, see [tryExecuteLocked] method description for details.
 *
 *
 * Note that [ID] is expected to implement [Any.equals] and [Any.hashCode] in a meaningful way.
 *
 *
 * See tests for usage examples.
 */
class EntityLocker<ID : Any> {

    private val lockedEntitiesHolder = LockedEntitiesHolder<ID>()

    private val waitingQueuesByEntityId = ConcurrentHashMap<ID, Queue<WaitingThreadDetails>>()

    private val waitingQueueLock = Semaphore(1)

    /**
     * Tries to acquire the lock for the specified entity,
     * then executes `protectedBlock` is executed under this lock and returns the result of that execution.
     *
     *
     * @return The result of the `protectedBlock`'s execution.
     *
     *
     * @throws [InterruptedException] if the current thread is interrupted.
     * @throws [IllegalMonitorStateException] in case of internal errors.
     */
    fun <R> executeLocked(entityId: ID, protectedBlock: () -> R): R {
        val currentThreadId = Thread.currentThread().id
        lock(entityId, currentThreadId)
        try {
            return protectedBlock()
        } finally {
            unlock(entityId, currentThreadId)
        }
    }

    /**
     * Tries to acquire the lock for the specified entity for the specified amount of time.
     * If the lock is acquired, then `protectedBlock` is executed under this lock and after that `true` is returned.
     * If the specified amount of time elapses prior to acquiring the lock,
     * then `protectedBlock` is not executed and `false` is returned.
     *
     *
     * @return `true` if lock was successfully acquired and `protectedBlock` was executed,
     * `false` if time elapsed prior to acquiring the lock.
     *
     *
     * @throws [InterruptedException] if the current thread is interrupted.
     * @throws [IllegalMonitorStateException] in case of internal errors.
     */
    fun tryExecuteLocked(entityId: ID, timeout: Long, timeoutUnit: TimeUnit, protectedBlock: () -> Unit): Boolean {
        val currentThreadId = Thread.currentThread().id
        if (!tryLock(entityId, timeout, timeoutUnit, currentThreadId)) {
            return false
        }
        try {
            protectedBlock()
            return true
        } finally {
            unlock(entityId, currentThreadId)
        }
    }

    private fun lock(entityId: ID, currentThreadId: Long) {
        if (lockedEntitiesHolder.tryLockAndStoreDetails(entityId, currentThreadId)) {
            return
        }
        val semaphoreToLockOn = recheckIsEntityLockedAndAddToQueueIfNeeded(entityId, currentThreadId)

        // Have to lock on the semaphore after releasing `waitingQueueLock`,
        // so this line cannot be moved into the method called above
        semaphoreToLockOn?.acquire()
    }

    private fun tryLock(entityId: ID, timeout: Long, timeoutUnit: TimeUnit, currentThreadId: Long): Boolean {
        if (lockedEntitiesHolder.tryLockAndStoreDetails(entityId, currentThreadId)) {
            return true
        }
        val semaphoreToLockOn = recheckIsEntityLockedAndAddToQueueIfNeeded(entityId, currentThreadId) ?: return true

        // Have to lock on the semaphore after releasing `waitingQueueLock`,
        // so this line cannot be moved into the method called above
        val acquired = semaphoreToLockOn.tryAcquire(1, timeout, timeoutUnit)
        if (acquired) {
            return true
        }
        return retryLockAndRemoveFromQueueIfNeeded(entityId, currentThreadId, semaphoreToLockOn)
    }

    private fun unlock(entityId: ID, currentThreadId: Long) {
        releaseNextLockedFromQueueIfExistsOrReleaseEntity(entityId, currentThreadId)
    }

    /**
     * @return a semaphore to lock on if the entity is locked, null if the entity is not locked.
     */
    private fun recheckIsEntityLockedAndAddToQueueIfNeeded(entityId: ID, currentThreadId: Long): Semaphore? {
        waitingQueueLock.acquire()
        try {
            if (lockedEntitiesHolder.tryLockAndStoreDetails(entityId, currentThreadId)) {
                // The entity has been released, so we do not need to wait
                return null
            }
            val lockedDetails = lockedEntitiesHolder.getLockedEntityDetails(entityId)

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

    private fun releaseNextLockedFromQueueIfExistsOrReleaseEntity(entityId: ID, currentThreadId: Long) {
        waitingQueueLock.acquire()
        try {
            val lockedEntityDetails = lockedEntitiesHolder.getLockedEntityDetails(entityId)

            if (lockedEntityDetails.holdingThreadId != currentThreadId) {
                // Should never happen as `lock()`/`unlock()` methods are private
                throw IllegalMonitorStateException("Trying to release entity with ID '$entityId' locked by another thread")
            }

            if (lockedEntityDetails.acquisitionsCount.decrementAndGet() > 0) {
                // The lock should be released at least once more, nothing to do
                return
            }

            val waitingQueue = waitingQueuesByEntityId[entityId]

            if (waitingQueue == null) {
                // Have to do it under the waitingQueueLock lock in case there is a concurrent lock request
                // that failed the fast track locking. As releasing (this line) is under the waitingQueueLock lock,
                // that request either have already added a waiting semaphore or will recheck the lockedEntityIds later.
                lockedEntitiesHolder.releaseEntityAndClearDetails(entityId)
                return
            }

            val nextWaitingThread = waitingQueue.poll()
                ?: // Should never happen
                throw IllegalMonitorStateException("Empty queue was not removed for entity with ID '$entityId'. This EntityLocker instance's state is corrupted")

            // There is a waiting thread
            if (waitingQueue.isEmpty()) {
                waitingQueuesByEntityId.remove(entityId)
            }
            lockedEntitiesHolder.replaceLockedEntityDetails(entityId, nextWaitingThread.threadId)
            // Have to release under the waitingQueueLock lock so that retrying in `retryLockAndRemoveFromQueueIfNeeded` shows the actual state
            nextWaitingThread.semaphoreToLockOn.release()
        } finally {
            waitingQueueLock.release()
        }
    }

    /**
     * @return `true` if locked successfully, false otherwise.
     */
    private fun retryLockAndRemoveFromQueueIfNeeded(
        entityId: ID,
        currentThreadId: Long,
        semaphoreToLockOn: Semaphore
    ): Boolean {
        waitingQueueLock.acquire()
        try {
            if (semaphoreToLockOn.tryAcquire(1, 0, TimeUnit.SECONDS)) {
                return true
            }

            // Failed to acquire, should remove the thread from the queue
            val waitingQueue = waitingQueuesByEntityId[entityId]
                ?: // Should never happen
                throw IllegalMonitorStateException("Entity with ID '$entityId' is expected to have at least one thread in the queue, but the queue is missing. This EntityLocker instance's state is corrupted")

            val waitingEntryRemoved = waitingQueue.removeIf { it.threadId == currentThreadId }

            if (!waitingEntryRemoved) {
                // Should never happen
                throw IllegalMonitorStateException("Current thread not found in the queue of the entity with ID '$entityId'. This EntityLocker instance's state is corrupted")
            }
            if (waitingQueue.isEmpty()) {
                waitingQueuesByEntityId.remove(entityId)
            }
            return false
        } finally {
            waitingQueueLock.release()
        }
    }
}
