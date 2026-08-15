package com.learning.order_service.repository

import com.learning.order_service.domain.Order
import org.springframework.data.jpa.repository.JpaRepository
import java.util.UUID

interface OrderRepository : JpaRepository<Order, Long> {
    fun findByOrderReference(orderReference: UUID): Order?
}