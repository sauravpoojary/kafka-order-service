package com.learning.order_service.event

import java.time.Instant
import java.util.UUID

data class OrderCreatedEvent (
    val orderReference: UUID,
    val customerId: String,
    val productName: String,
    val quantity: Int,
    val amount: Double,
    val occurredAt: Instant = Instant.now()
)