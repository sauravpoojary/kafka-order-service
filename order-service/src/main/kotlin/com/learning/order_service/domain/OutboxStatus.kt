package com.learning.order_service.domain

enum class OutboxStatus {
    PENDING,
    PUBLISHED,
    FAILED
}