package me.orekin.entitylocker

import kotlin.test.Test

internal class EntityLockerTest {

    private val locker = EntityLocker<Int>()

    @Test
    fun testLocker() {
        locker.executeLocked(1) {}
    }

}