package com.buzz.kotkafka.consumer

import com.buzz.kotkafka.common.KafkaConstants
import kotlinx.coroutines.*
import mu.KotlinLogging
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.annotation.TopicPartition
import org.springframework.kafka.support.Acknowledgment
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

@Component
class BuzzTopicListener(
    private val coroutineScope: CoroutineScope
) {

    private val logger = KotlinLogging.logger {}

    init {
        logger.info { "BuzzTopicListener Created..." }
    }

    @KafkaListener(
        id = "consumePartion1",
        topicPartitions = [
            TopicPartition(topic = KafkaConstants.TEST_TOPIC_NAME, partitions = ["0"])
        ] ,
        groupId = KafkaConstants.TEST_GROUP1_NAME
    )
    fun consumePartion1(@Payload data: String,ack: Acknowledgment) {
        coroutineScope.launch {
            try {
                logger.info("Partion[0] Message")
                processMessage(data)
                ack.acknowledge()
            } catch (e: Exception) {
                // 에러 처리
                logger.error("Error processing message: ${e.message}")
            }
        }

    }

    @KafkaListener(
        id = "consumePartion2",
        topicPartitions = [
            TopicPartition(topic = KafkaConstants.TEST_TOPIC_NAME, partitions = ["1"])
        ],
        groupId = KafkaConstants.TEST_GROUP1_NAME
    )
    fun consumePartion2(@Payload data: String,ack: Acknowledgment) {

        coroutineScope.launch {
            try {
                logger.info("Partion[1] Message")
                processMessage(data)
                ack.acknowledge()
            } catch (e: Exception) {
                // 에러 처리
                logger.error("Error processing message: ${e.message}")
            }
        }
    }

    @KafkaListener(
        id = "consumePartion3",
        topicPartitions = [
            TopicPartition(topic = KafkaConstants.TEST_TOPIC_NAME, partitions = ["2"])
        ],
        groupId = KafkaConstants.TEST_GROUP1_NAME
    )
    fun consumePartion3(@Payload data: String,ack: Acknowledgment) {
        coroutineScope.launch {
            try {
                logger.info("Partion[2] Message")
                processMessage(data)
                ack.acknowledge()
            } catch (e: Exception) {
                // 에러 처리
                logger.error("Error processing message: ${e.message}")
            }
        }
    }

    private suspend fun processMessage(message: String) {
        withContext(Dispatchers.Default) {
            // 메시지 처리 로직
            logger.info("Processing message: $message")
            delay(1000) // 시뮬레이션된 처리 시간
            logger.info("Processed message: $message")
        }
    }
}