package com.learning.order_service.kafka.consumer

import com.learning.order_service.domain.OrderStatus
import com.learning.order_service.event.OrderConfirmedEvent
import com.learning.order_service.repository.OrderRepository
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component

@Component
class OrderConfirmationListener(
    private val orderRepository: OrderRepository
) {
    private val logger = LoggerFactory.getLogger(OrderConfirmationListener::class.java)

    @KafkaListener(
        topics = ["order-confirmed"],
        groupId = "order-status-update-group"
    )
    fun onOrderConfirmed(record: ConsumerRecord<String, OrderConfirmedEvent>, ack: Acknowledgment) {
        val event = record.value()
        MDC.put("orderReference", event.orderReference.toString())
        // TEMPORARY — force failure to test DLT routing
        // throw RuntimeException("Simulated processing failure for DLT test")
        try {
            val rowsUpdated = orderRepository.updateStatus(event.orderReference, OrderStatus.CONFIRMED)
            if (rowsUpdated == 0) {
                logger.warn("No order found for reference={} — nothing updated", event.orderReference)
            } else {
                logger.info("Order status updated to CONFIRMED reference={}", event.orderReference)
            }
            ack.acknowledge()
        } finally {
            MDC.clear()
        }
    }
}