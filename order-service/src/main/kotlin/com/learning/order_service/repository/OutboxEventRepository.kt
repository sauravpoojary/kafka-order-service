package com.learning.order_service.repository

import com.learning.order_service.domain.OutboxEvent
import com.learning.order_service.domain.OutboxStatus
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.domain.Pageable

interface OutboxEventRepository : JpaRepository<OutboxEvent, Long> {
    fun findByStatusOrderByCreatedAtAsc(status: OutboxStatus, pageable: Pageable): List<OutboxEvent>
}