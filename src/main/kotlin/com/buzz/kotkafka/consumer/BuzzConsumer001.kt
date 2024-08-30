package com.buzz.kotkafka.consumer

import com.buzz.kotkafka.common.KafkaConstants
import com.buzz.kotkafka.dto.SendMessage
import com.google.gson.Gson
import kotlinx.coroutines.delay
import mu.KotlinLogging
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.annotation.TopicPartition
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

/*
    Kafka 의 Consumer 는 Thread Not Safe 함
    offset commits 은  poll()ing the consumer 과 동일한 쓰레드에서 발생해야 한다.
 */
@Component
class BuzzConsumer001 {
    private val logger = KotlinLogging.logger {}
    private val gson = Gson()

    init {
        logger.info { "BuzzSyncConsumer Created..." }
    }

    @KafkaListener(
        clientIdPrefix = "buzzConsumer",
        topics = [KafkaConstants.TEST_TOPIC_NAME],
        groupId = KafkaConstants.TEST_GROUP1_NAME,
        concurrency = "3"
    )
    fun syncConsumer1(@Payload data: String,
                      @Header(KafkaHeaders.OFFSET) offset : Long,
                      @Header(KafkaHeaders.RECEIVED_PARTITION) partition : Int,
                      ack: Acknowledgment) {
        try {
            logger.info("Receive Message 1: $data" )
            //TO-DO List...

            val sendMessage = gson.fromJson(data, SendMessage::class.java)
            //val result =  processMessage(data)
            val commitList = listOf(1,3,4)
            if(sendMessage.keyNum in commitList) {
                ack.acknowledge()
                logger.info("COMMIT offset... : $offset" )
            }else{
                logger.error("NOT COMMIT offset... : $offset" )
            }

        } catch (e: Exception) {
            // 에러 처리
            logger.error("Error processing message: ${e.message}")
        }
    }

    fun  processMessage(message: String) : Pair<Boolean, String> {

        val range = (1..5)
        return Pair(range.random() > 3,"");
    }

}