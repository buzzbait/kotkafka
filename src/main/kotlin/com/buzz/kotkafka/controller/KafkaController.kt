package com.buzz.kotkafka.controller

import com.buzz.kotkafka.common.KafkaConstants
import com.buzz.kotkafka.dto.SendMessage
import com.buzz.kotkafka.service.KafkaProducerService
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/kafka")
class KafkaController (
    private val kafkaProducerService : KafkaProducerService
){

    @PostMapping("")
    fun sendMessage() : ResponseEntity<String> {

        val sendMessage =  SendMessage("kafkaProducerService","This is Send Message")
        for (i in 1..50) {
            kafkaProducerService.sendMessage(KafkaConstants.TEST_TOPIC_NAME,sendMessage)
        }

        return ResponseEntity.ok("SEND_OK")
    }
}