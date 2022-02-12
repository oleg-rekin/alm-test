package me.orekin.entitylocker

import java.util.Queue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Semaphore
import java.util.concurrent.locks.ReentrantLock

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
        // Another thread can release the last lock here, TODO recheck
        val semaphoreToLockOn = addLockedToQueue(entityId)
        semaphoreToLockOn.acquire()
    }

    private fun unlock(entityId: ID) {
        val semaphoreToFree = getNextLockedFromQueueIfExists(entityId)
        if (semaphoreToFree == null) {
            lockedEntityIds.remove(entityId)
            return
        }
        semaphoreToFree.release()
    }

    private fun addLockedToQueue(entityId: ID): Semaphore {
        waitingQueueLock.lock()
        try {
            val waitingQueue = waitingQueuesByEntityId.computeIfAbsent(entityId) { ConcurrentLinkedQueue() }
            val semaphore = Semaphore(0)
            waitingQueue.add(semaphore)
            return semaphore
        } finally {
            waitingQueueLock.unlock()
        }
    }

    private fun getNextLockedFromQueueIfExists(entityId: ID): Semaphore? {
        waitingQueueLock.lock()
        try {
            val waitingQueue = waitingQueuesByEntityId[entityId] ?: return null

            if (waitingQueue.isEmpty()) {
                // Should never happen, TODO use proper logging
                println("Empty queue was not removed for entity ID $entityId, removing it now")
                waitingQueuesByEntityId.remove(entityId)
            }

            val semaphore = waitingQueue.remove()
            if (waitingQueue.isEmpty()) {
                waitingQueuesByEntityId.remove(entityId)
            }
            return semaphore
        } finally {
            waitingQueueLock.unlock()
        }
    }
}
