package com.buzz.kotkafka.service

import mu.KotlinLogging
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service

@Service
class KafkaProducerService(
    private val kafkaTemplate: KafkaTemplate<String, Any>
){
    private val logger = KotlinLogging.logger {}

    //비동기 전송
    fun sendMessage(topic: String, message: Any) {
        kafkaTemplate.send(topic, message)
            .whenComplete { result, exception ->
                if (exception != null) {
                    logger.error("ERROR sending to Kafka", exception)
                } else {
                    val metadata = result.recordMetadata
                    logger.info("Message sent to topic ${metadata.topic()}, partition: ${metadata.partition()}, offset: ${metadata.offset()}")
                }
            }
    }
}