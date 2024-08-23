package com.buzz.kotkafka.consumer

import com.buzz.kotkafka.common.KafkaConstants
import mu.KotlinLogging
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.annotation.TopicPartition
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

@Component
class BuzzTopicListener {

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
    fun consumePartion1(@Payload data: String) {
        logger.info("Partion[0] Message: $data")
    }

    @KafkaListener(
        id = "consumePartion2",
        topicPartitions = [
            TopicPartition(topic = KafkaConstants.TEST_TOPIC_NAME, partitions = ["1"])
        ],
        groupId = KafkaConstants.TEST_GROUP1_NAME
    )
    fun consumePartion2(@Payload data: String) {

        logger.info("Partion[1] Message: $data")
    }

    @KafkaListener(
        id = "consumePartion3",
        topicPartitions = [
            TopicPartition(topic = KafkaConstants.TEST_TOPIC_NAME, partitions = ["2"])
        ],
        groupId = KafkaConstants.TEST_GROUP1_NAME
    )
    fun consumePartion3(@Payload data: String) {

        logger.info("Partion[2] Message: $data")
    }
}