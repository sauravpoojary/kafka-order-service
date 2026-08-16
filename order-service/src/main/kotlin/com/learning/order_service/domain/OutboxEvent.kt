package com.learning.order_service.domain

import jakarta.persistence.*
import java.time.Instant

@Entity
@Table(name = "outbox_events")
class OutboxEvent(

    @Column(nullable = false)
    val aggregateType: String,

    @Column(nullable = false)
    val aggregateId: String,

    @Column(nullable = false)
    val eventType: String,

    @Column(nullable = false)
    val topic: String,

    @Column(nullable = false, columnDefinition = "TEXT")
    val payload: String,

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    var status: OutboxStatus = OutboxStatus.PENDING,

    @Column(nullable = false)
    var attemptCount: Int = 0,

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long? = null,

    @Column(nullable = false, updatable = false)
    val createdAt: Instant = Instant.now(),

    var publishedAt: Instant? = null
)