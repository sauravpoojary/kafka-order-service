package com.learning.order_service.dto

import com.learning.order_service.domain.Order
import com.learning.order_service.domain.OrderStatus
import java.time.Instant
import java.util.*

data class OrderResponse(
    val orderReference: UUID,
    val customerId: String,
    val productName: String,
    val quantity: Int,
    val amount: Double,
    val status: OrderStatus,
    val createdAt: Instant
) {
    companion object {
        fun fromEntity(order: Order): OrderResponse = OrderResponse(
            orderReference = order.orderReference,
            customerId = order.customerId,
            productName = order.productName,
            quantity = order.quantity,
            amount = order.amount,
            status = order.status,
            createdAt = order.createdAt
        )
    }
}