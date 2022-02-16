package me.orekin.entitylocker

import java.util.concurrent.atomic.AtomicLong

internal data class LockedEntityDetails(
    val holdingThreadId: Long,
    val acquisitionsCount: AtomicLong
)