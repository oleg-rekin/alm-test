package me.orekin.entitylocker

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock

internal class LockedEntitiesHolder<ID : Any> {

    private val lockedEntityIds = ConcurrentHashMap.newKeySet<ID>()

    private val lockedEntityDetailsById = ConcurrentHashMap<ID, LockedEntityDetails>()

    private val lockedEntitiesLock = ReentrantLock()

    /**
     * Details are stored only in case of successful locking.
     *
     *
     * @return `true` if locked successfully, `false` otherwise.
     */
    fun tryLockAndStoreDetails(entityId: ID, currentThreadId: Long): Boolean {
        lockedEntitiesLock.lock()
        try {
            if (!lockedEntityIds.add(entityId)) {
                return false
            }
            lockedEntityDetailsById[entityId] = LockedEntityDetails(currentThreadId, AtomicLong(1))
            return true
        } finally {
            lockedEntitiesLock.unlock()
        }
    }

    fun releaseEntityAndClearDetails(entityId: ID) {
        lockedEntitiesLock.lock()
        try {
            lockedEntityIds.remove(entityId)
            lockedEntityDetailsById.remove(entityId)
        } finally {
            lockedEntitiesLock.unlock()
        }
    }

    /**
     * Assumes that the entity is locked.
     */
    fun getLockedEntityDetails(entityId: ID): LockedEntityDetails {
        return lockedEntityDetailsById[entityId]
            ?: // Should never happen
            throw IllegalMonitorStateException("Entity with ID '$entityId' is locked but there is no details on the lock. This EntityLocker instance's state is corrupted")
    }

    /**
     * Assumes that the entity is locked.
     */
    fun replaceLockedEntityDetails(entityId: ID, threadId: Long) {
        lockedEntityDetailsById[entityId] = LockedEntityDetails(threadId, AtomicLong(1))
    }
}