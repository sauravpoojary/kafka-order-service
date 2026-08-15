package com.learning.order_service.kafka.producer

import com.learning.order_service.event.OrderCreatedEvent
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class OrderEventProducer(
    private val kafkaTemplate: KafkaTemplate<String, OrderCreatedEvent>
) {
    private val logger = LoggerFactory.getLogger(OrderEventProducer::class.java)

    companion object {
        const val ORDER_CREATED_TOPIC = "order_created"
    }

    fun publishOrderCreated(orderCreatedEvent: OrderCreatedEvent){
        val key = orderCreatedEvent.orderReference.toString()

        kafkaTemplate.send(ORDER_CREATED_TOPIC, key, orderCreatedEvent)
            .whenComplete { result, exception ->
                if(exception != null){
                    logger.error("Failed to publish OrderCreatedEvent for order={}", key, exception)
                }else {
                    logger.info(
                        "Published OrderCreatedEvent order={} partition={} offset={}",
                        key,
                        result.recordMetadata.partition(),
                        result.recordMetadata.offset()
                    )
                }
            }
    }
}