package me.orekin.entitylocker

import kotlin.test.Test
import kotlin.test.assertNotNull

internal class EntityLockerTest {

    private val locker = EntityLocker()

    @Test
    fun testLocker() {
        assertNotNull(locker)
    }

}