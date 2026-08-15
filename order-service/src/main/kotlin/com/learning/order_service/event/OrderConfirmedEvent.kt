package com.learning.order_service.event

import java.time.Instant
import java.util.UUID

data class OrderConfirmedEvent(
    val orderReference: UUID,
    val confirmedAt: Instant = Instant.now()
)