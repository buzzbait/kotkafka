package com.buzz.kotkafka.consumer

import com.buzz.kotkafka.common.KafkaConstants
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import mu.KotlinLogging
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.annotation.TopicPartition
import org.springframework.kafka.support.Acknowledgment
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

@Component
class BuzzConsumer003 {
    private val logger = KotlinLogging.logger {}

    init {
        logger.info { "BuzzAsyncConsumer Created..." }
    }

    @KafkaListener(
        clientIdPrefix = "buzzConsumer",
        topics = [KafkaConstants.TEST_TOPIC_NAME],
        groupId = KafkaConstants.TEST_GROUP1_NAME,
        concurrency = "3"
    )
    /*fun consumerOnReceive(@Payload message: String, ack: Acknowledgment) {
        // Asynchronous processing logic
        logger.info("Message Receive : $message")
        //ack.acknowledge()
    }*/
    suspend fun consumerOnReceive(@Payload message: String, ack: Acknowledgment) = coroutineScope {
        launch {
            // Asynchronous processing logic
            logger.info("Message Receive : $message")
            val result =  processMessage(message)
            ack.acknowledge()
        }
    }

    private suspend fun  processMessage(message: String) : Pair<Boolean, String> {
        delay(1000) // 시뮬레이션된 처리 시간
        logger.info("Processing End...: $message")
        return Pair(true,"");
    }

}