package com.learning.order_service.kafka.consumer

import com.learning.order_service.event.OrderConfirmedEvent
import com.learning.order_service.event.OrderCreatedEvent
import com.learning.order_service.kafka.producer.OrderEventProducer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component

@Component
class NotificationListener(
    private val orderEventProducer: OrderEventProducer
) {
    private val logger = LoggerFactory.getLogger(NotificationListener::class.java)

    @KafkaListener(
        topics = ["order-created"],
        groupId = "notification-service-group"
    )
    fun onOrderCreated(record: ConsumerRecord<String, OrderCreatedEvent>, ack: Acknowledgment) {
        val event = record.value()
        MDC.put("orderReference", event.orderReference.toString())
        try {
            logger.info("Simulating notification send for order={}", event.orderReference)
            Thread.sleep(200)
            orderEventProducer.publish(
                OrderEventProducer.ORDER_CONFIRMED_TOPIC,
                event.orderReference.toString(),
                OrderConfirmedEvent(orderReference = event.orderReference)
            )
            ack.acknowledge()
        } finally {
            MDC.clear()
        }
    }
}