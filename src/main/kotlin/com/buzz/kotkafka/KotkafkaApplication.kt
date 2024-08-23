package com.buzz.kotkafka

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KotkafkaApplication

fun main(args: Array<String>) {
	runApplication<KotkafkaApplication>(*args)
}
