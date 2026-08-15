package com.learning.order_service.repository

import com.learning.order_service.domain.Order
import com.learning.order_service.domain.OrderStatus
import jakarta.transaction.Transactional
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import java.time.Instant
import java.util.UUID

interface OrderRepository : JpaRepository<Order, Long> {
    fun findByOrderReference(orderReference: UUID): Order?

    @Modifying
    @Transactional
    @Query(
        """
        UPDATE Order o
        SET o.status = :status, o.updatedAt = :updatedAt
        WHERE o.orderReference = :orderReference
        """
    )
    fun updateStatus(
        @Param("orderReference") orderReference: UUID,
        @Param("status") status: OrderStatus,
        @Param("updatedAt") updatedAt: Instant = Instant.now()
    ): Int
}