package com.buzz.kotkafka.consumer

import com.buzz.kotkafka.common.KafkaConstants
import kotlinx.coroutines.*
import mu.KotlinLogging
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.annotation.TopicPartition
import org.springframework.kafka.support.Acknowledgment
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

/*
    코루틴 안에서 Acknowledgment 의 acknowledge() 를 호출하는 경우 정상적으로 커밋이 안되는것 같음
    재시작시 몇개의 메시지가 다시 들어 옴(커밋이 안된 레코드들)
    코루틴 밖에서 호출하는 경우는 모두 커밋됨
 */
@Component
class BuzzAsyncConsumer(
    private val coroutineScope: CoroutineScope
) {
    private val logger = KotlinLogging.logger {}

    init {
        logger.info { "BuzzAsyncConsumer Created..." }
    }

    @KafkaListener(
        clientIdPrefix = "buzzConsumer",
        topicPartitions = [
            TopicPartition(topic = KafkaConstants.TEST_TOPIC_NAME, partitions = ["0"])
        ] ,
        groupId = KafkaConstants.TEST_GROUP1_NAME
    )
    fun consumePartition1(@Payload message: String,ack: Acknowledgment){
        CoroutineScope(Dispatchers.IO).launch {
            logger.info("Partition[1] Message : $message")
            processMessage(message)
            ack.acknowledge()
        }
    }


    @KafkaListener(
        clientIdPrefix = "buzzConsumer",
        topicPartitions = [
            TopicPartition(topic = KafkaConstants.TEST_TOPIC_NAME, partitions = ["1"])
        ],
        groupId = KafkaConstants.TEST_GROUP1_NAME
    )
    fun consumePartition2(@Payload message: String,ack: Acknowledgment){
        CoroutineScope(Dispatchers.IO).launch {
            logger.info("Partition[2] Message : $message")
            processMessage(message)
            ack.acknowledge()
        }
    }

    @KafkaListener(
        clientIdPrefix = "buzzConsumer",
        topicPartitions = [
            TopicPartition(topic = KafkaConstants.TEST_TOPIC_NAME, partitions = ["2"])
        ],
        groupId = KafkaConstants.TEST_GROUP1_NAME
    )
    fun consumePartition3(@Payload message: String,ack: Acknowledgment){
        CoroutineScope(Dispatchers.IO).launch {
            logger.info("Partition[3] Message : $message")
            processMessage(message)
            ack.acknowledge()
        }
    }

    private suspend fun  processMessage(message: String) : Pair<Boolean, String> {

        logger.info("Processing kafka Message...: $message")
        delay(1000) // 시뮬레이션된 처리 시간
        return Pair(true,"");
        /*withContext(Dispatchers.Default) {
            // 메시지 처리 로직
            logger.info("Processing Start...: $message")
            delay(1000) // 시뮬레이션된 처리 시간
            logger.info("Processed End...: $message")
        }*/
    }
}