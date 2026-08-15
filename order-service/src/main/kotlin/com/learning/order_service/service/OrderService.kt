package com.learning.order_service.service

import com.learning.order_service.domain.Order
import com.learning.order_service.dto.CreateOrderRequest
import com.learning.order_service.dto.OrderResponse
import com.learning.order_service.event.OrderCreatedEvent
import com.learning.order_service.kafka.producer.OrderEventProducer
import com.learning.order_service.repository.OrderRepository
import jakarta.transaction.Transactional
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class OrderService(
    private val orderRepository: OrderRepository,
    private val orderEventProducer: OrderEventProducer
) {
    private val logger = LoggerFactory.getLogger(OrderService::class.java)


    @Transactional
    fun createOrder(createOrderRequest: CreateOrderRequest): OrderResponse {

        val order: Order = Order(
            customerId = createOrderRequest.customerId,
            productName = createOrderRequest.productName,
            quantity = createOrderRequest.quantity,
            amount = createOrderRequest.amount
        )

        val savedOrder = orderRepository.save(order)

        logger.info("Order persisted id={} reference={}", savedOrder.id, savedOrder.orderReference)

        orderEventProducer.publishOrderCreated(
            OrderCreatedEvent(
                orderReference = savedOrder.orderReference,
                customerId = savedOrder.customerId,
                productName = savedOrder.productName,
                quantity = savedOrder.quantity,
                amount = savedOrder.amount
            )
        )

        return OrderResponse.fromEntity(savedOrder)

    }
}