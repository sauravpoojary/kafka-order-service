package com.learning.order_service.kafka.producer

import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import java.util.concurrent.TimeUnit

@Component
class OrderEventProducer(
    private val kafkaTemplate: KafkaTemplate<String, Any>
) {
    private val logger = LoggerFactory.getLogger(OrderEventProducer::class.java)

    companion object {
        const val ORDER_CREATED_TOPIC = "order-created"
        const val ORDER_CONFIRMED_TOPIC = "order-confirmed"
    }

    fun publish(topic: String, key: String, event: Any) {
        kafkaTemplate.send(topic, key, event)
            .whenComplete { result, exception ->
                if (exception != null) {
                    logger.error("Failed to publish event topic={} key={}", topic, key, exception)
                } else {
                    logger.info(
                        "Published event topic={} key={} partition={} offset={}",
                        topic, key, result.recordMetadata.partition(), result.recordMetadata.offset()
                    )
                }
            }
    }

    fun publishSync(topic: String, key: String, event: Any) {
        // .get() blocks until Kafka acks or throws — the poller needs to KNOW
        // it succeeded before marking the outbox row PUBLISHED.
        kafkaTemplate.send(topic, key, event).get(5, TimeUnit.SECONDS)
    }
}