package me.orekin.entitylocker

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue

class EntityLocker<ID> {

    private val lockedEntityIds = ConcurrentHashMap.newKeySet<ID>()

    private val waitingRequestsByEntityId = ConcurrentHashMap<ID, ConcurrentLinkedQueue<Boolean>>()

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
        TODO()
    }

    private fun unlock(entityId: ID) {
        TODO()
        lockedEntityIds.remove(entityId)
    }
}