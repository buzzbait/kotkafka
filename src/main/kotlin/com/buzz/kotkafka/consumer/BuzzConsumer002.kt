package com.buzz.kotkafka.consumer

import com.buzz.kotkafka.common.KafkaConstants
import kotlinx.coroutines.*
import mu.KotlinLogging
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.annotation.TopicPartition
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component
import java.time.Duration

/*
    코루틴 안에서 Acknowledgment 의 acknowledge() 를 호출하는 경우 정상적으로 커밋이 안되는것 같음
    재시작시 몇개의 메시지가 다시 들어 옴(커밋이 안된 레코드들)
    코루틴 밖에서 호출하는 경우는 모두 커밋됨
 */
//@Component
class BuzzConsumer002{
    private val logger = KotlinLogging.logger {}

    init {
        logger.info { "BuzzAsyncConsumer Created..." }
    }


    /*
         Spring Kafka 3.2+ 이상에서는 @KafkaListener 에 suspend 함수 사용 가능
     */
    @KafkaListener(
        clientIdPrefix = "buzzConsumer",
        topics = [KafkaConstants.TEST_TOPIC_NAME],
        groupId = KafkaConstants.TEST_GROUP1_NAME,
        concurrency = "3"
    )
    suspend fun onReceiveMessage(@Payload message: String,
                                  @Header(KafkaHeaders.OFFSET) offset : Long,
                                  @Header(KafkaHeaders.RECEIVED_PARTITION) partition : Int,
                                  ack: Acknowledgment) = coroutineScope {
        launch {
            // Asynchronous processing logic
            logger.info("Partition :[$partition]-Offset:[$offset] >> Message : $message")
            val result =  processMessage(message)
            if(result.first) {
                ack.acknowledge()
            }

        }
    }


    private suspend fun  processMessage(message: String) : Pair<Boolean, String> {

        val range = (1..5)

        logger.info("Processing kafka Message...: $message")
        delay(1000) // 시뮬레이션된 처리 시간
        return Pair(range.random() > 3,"");
        /*withContext(Dispatchers.Default) {
            // 메시지 처리 로직
            logger.info("Processing Start...: $message")
            delay(1000) // 시뮬레이션된 처리 시간
            logger.info("Processed End...: $message")
        }*/
    }
}