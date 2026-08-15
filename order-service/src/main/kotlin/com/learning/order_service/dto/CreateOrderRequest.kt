package com.learning.order_service.dto

import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.Positive

data class CreateOrderRequest (
    @field:NotBlank(message = "customerId is required")
    val customerId: String,

    @field:NotBlank(message = "productName is required")
    val productName: String,

    @field:Positive(message = "quantity must be positive")
    val quantity: Int,

    @field:Positive(message = "amount must be positive")
    val amount: Double
)