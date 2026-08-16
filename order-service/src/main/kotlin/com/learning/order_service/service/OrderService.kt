package com.learning.order_service.service

import com.learning.order_service.domain.Order
import com.learning.order_service.domain.OutboxEvent
import com.learning.order_service.dto.CreateOrderRequest
import com.learning.order_service.dto.OrderResponse
import com.learning.order_service.event.OrderCreatedEvent
import com.learning.order_service.kafka.producer.OrderEventProducer
import com.learning.order_service.repository.OrderRepository
import com.learning.order_service.repository.OutboxEventRepository
import jakarta.transaction.Transactional
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import tools.jackson.databind.ObjectMapper

@Service
class OrderService(
    private val orderRepository: OrderRepository,
    private val outboxEventRepository: OutboxEventRepository,
    private val objectMapper: ObjectMapper
) {
    private val logger = LoggerFactory.getLogger(OrderService::class.java)

    @Transactional
    fun createOrder(request: CreateOrderRequest): OrderResponse {
        val order = Order(
            customerId = request.customerId,
            productName = request.productName,
            quantity = request.quantity,
            amount = request.amount
        )
        val saved = orderRepository.save(order)
        logger.info("Order persisted id={} reference={}", saved.id, saved.orderReference)

        val event = OrderCreatedEvent(
            orderReference = saved.orderReference,
            customerId = saved.customerId,
            productName = saved.productName,
            quantity = saved.quantity,
            amount = saved.amount
        )

        // Same transaction as the order save above — this is the whole point.
        outboxEventRepository.save(
            OutboxEvent(
                aggregateType = "Order",
                aggregateId = saved.orderReference.toString(),
                eventType = "ORDER_CREATED",
                topic = OrderEventProducer.ORDER_CREATED_TOPIC,
                payload = objectMapper.writeValueAsString(event)
            )
        )

        return OrderResponse.fromEntity(saved)
    }
}