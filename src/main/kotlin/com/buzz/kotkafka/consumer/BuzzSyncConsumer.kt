package com.buzz.kotkafka.consumer

import com.buzz.kotkafka.common.KafkaConstants
import mu.KotlinLogging
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.annotation.TopicPartition
import org.springframework.kafka.support.Acknowledgment
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

/*
    Kafka 의 Consumer 는 Thread Not Safe 함
    offset commits 은  poll()ing the consumer 과 동일한 쓰레드에서 발생해야 한다.
 */
//@Component
class BuzzSyncConsumer {
    private val logger = KotlinLogging.logger {}

    init {
        logger.info { "BuzzSyncConsumer Created..." }
    }

    @KafkaListener(
        clientIdPrefix = "BuzzSyncConsumer",
        topicPartitions = [
            TopicPartition(topic = KafkaConstants.TEST_TOPIC_NAME, partitions = ["0"])
        ] ,
        groupId = KafkaConstants.TEST_GROUP1_NAME
    )
    fun syncConsumer1(@Payload data: String, ack: Acknowledgment) {
        try {
            logger.info("Receive Message 1: $data" )
            //TO-DO List...
            ack.acknowledge()
        } catch (e: Exception) {
            // 에러 처리
            logger.error("Error processing message: ${e.message}")
        }
    }

    @KafkaListener(
        clientIdPrefix = "BuzzSyncConsumer",
        topicPartitions = [
            TopicPartition(topic = KafkaConstants.TEST_TOPIC_NAME, partitions = ["1"])
        ] ,
        groupId = KafkaConstants.TEST_GROUP1_NAME
    )
    fun syncConsumer2(@Payload data: String, ack: Acknowledgment) {
        try {
            logger.info("Receive Message 2: $data" )
            //TO-DO List...
            ack.acknowledge()
        } catch (e: Exception) {
            // 에러 처리
            logger.error("Error processing message: ${e.message}")
        }
    }

    @KafkaListener(
        clientIdPrefix = "BuzzSyncConsumer",
        topicPartitions = [
            TopicPartition(topic = KafkaConstants.TEST_TOPIC_NAME, partitions = ["2"])
        ] ,
        groupId = KafkaConstants.TEST_GROUP1_NAME
    )
    fun syncConsumer3(@Payload data: String, ack: Acknowledgment) {
        try {
            logger.info("Receive Message 3: $data" )
            //TO-DO List...
            ack.acknowledge()
        } catch (e: Exception) {
            // 에러 처리
            logger.error("Error processing message: ${e.message}")
        }
    }
}