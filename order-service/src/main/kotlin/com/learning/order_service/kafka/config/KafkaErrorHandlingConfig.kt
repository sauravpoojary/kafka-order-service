package com.learning.order_service.kafka.config

import org.apache.kafka.common.TopicPartition
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.KafkaOperations
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.support.serializer.DeserializationException
import org.springframework.util.backoff.FixedBackOff

@Configuration
class KafkaErrorHandlingConfig {

    @Bean
    fun deadLetterPublishingRecoverer(kafkaTemplate: KafkaOperations<String, Any>): DeadLetterPublishingRecoverer =
        DeadLetterPublishingRecoverer(kafkaTemplate) { record, _ ->
            TopicPartition("${record.topic()}-dlt", -1)
        }

    @Bean
    fun kafkaErrorHandler(recoverer: DeadLetterPublishingRecoverer): DefaultErrorHandler {
        val backOff = FixedBackOff(1000, 3)
        val handler = DefaultErrorHandler(recoverer, backOff)
        handler.addNotRetryableExceptions(DeserializationException::class.java)
        return handler
    }


}