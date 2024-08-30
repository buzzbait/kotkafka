package com.buzz.kotkafka.consumer

import com.buzz.kotkafka.common.KafkaConstants
import kotlinx.coroutines.*
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.listener.AcknowledgingMessageListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component
import org.w3c.dom.ranges.Range

//@Component
class BuzzConsumer003 : AcknowledgingMessageListener<String, String> {
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
    override fun onMessage( data: ConsumerRecord<String, String>, acknowledgment: Acknowledgment?) {

        CoroutineScope(Dispatchers.IO).launch {
            logger.info("Message : ${data.value()}")
            val result = processMessage(data.value())
            if(result.first) acknowledgment?.acknowledge() else logger.info("NOT Commit...")
        }
    }
    private suspend fun  processMessage(message: String) : Pair<Boolean, String> {
        val range = (1..5)
        delay(1000) // 시뮬레이션된 처리 시간
        logger.info("Processing End...: $message")
        //return Pair(true,"");
        return Pair(range.random() > 3,"");
    }



}