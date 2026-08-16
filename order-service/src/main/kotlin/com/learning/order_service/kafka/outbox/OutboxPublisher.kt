package com.learning.order_service.kafka.outbox

import com.learning.order_service.domain.OutboxStatus
import com.learning.order_service.event.OrderCreatedEvent
import com.learning.order_service.kafka.producer.OrderEventProducer
import com.learning.order_service.repository.OutboxEventRepository
import org.slf4j.LoggerFactory
import org.springframework.data.domain.PageRequest
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import tools.jackson.databind.ObjectMapper

@Component
class OutboxPublisher(
    private val outboxEventRepository: OutboxEventRepository,
    private val orderEventProducer: OrderEventProducer,
    private val objectMapper: ObjectMapper
) {
    private val logger = LoggerFactory.getLogger(OutboxPublisher::class.java)

    private val eventTypeToClass: Map<String, Class<*>> = mapOf(
        "ORDER_CREATED" to OrderCreatedEvent::class.java
    )

    @Scheduled(fixedDelay = 2000)
    @Transactional
    fun publishPendingEvents() {
        val batch = outboxEventRepository.findByStatusOrderByCreatedAtAsc(
            OutboxStatus.PENDING, PageRequest.of(0, 50)
        )
        if (batch.isEmpty()) return

        for (outboxEvent in batch) {
            try {
                val clazz = eventTypeToClass[outboxEvent.eventType]
                    ?: error("Unknown outbox eventType: ${outboxEvent.eventType}")
                val event = objectMapper.readValue(outboxEvent.payload, clazz)

                // Synchronous send — we only mark PUBLISHED once Kafka actually confirms.
                orderEventProducer.publishSync(outboxEvent.topic, outboxEvent.aggregateId, event)

                outboxEvent.status = OutboxStatus.PUBLISHED
                outboxEvent.publishedAt = java.time.Instant.now()
            } catch (ex: Exception) {
                outboxEvent.attemptCount += 1
                logger.error(
                    "Failed to publish outbox event id={} attempt={}",
                    outboxEvent.id, outboxEvent.attemptCount, ex
                )
                if (outboxEvent.attemptCount >= 5) {
                    outboxEvent.status = OutboxStatus.FAILED
                    logger.error("Outbox event id={} marked FAILED after 5 attempts", outboxEvent.id)
                }
            }
        }
        outboxEventRepository.saveAll(batch)
    }
}