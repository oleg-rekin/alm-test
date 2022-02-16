package me.orekin.entitylocker

import java.util.concurrent.Semaphore

internal data class WaitingThreadDetails(
    val threadId: Long,
    val semaphoreToLockOn: Semaphore
)
