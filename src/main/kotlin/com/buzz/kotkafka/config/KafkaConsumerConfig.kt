package com.buzz.kotkafka.config

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.ContainerProperties


@EnableKafka
@Configuration
class KafkaConsumerConfig {

    private val logger = KotlinLogging.logger {}
    private val CONSUMER_GROUP_ID = "buzz-consumer";

    //객체 변수를 null 로 선언 불가 하여 나중에 초기화 한다는 의미에서 lateinit 사용
    @Value("\${spring.kafka.consumer.bootstrap-servers}")
    private lateinit var BOOTSTRAP_SERVER: String

    init {
        logger.info("KafkaConsumerConfig Created...")
    }

    @Bean
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()

        factory.setConcurrency(2) // Consumer Process Thread Count
        factory.consumerFactory = DefaultKafkaConsumerFactory(getConfig())
        //factory.containerProperties.pollTimeout = 500
        // 리스너에서 acknowledgment가 호출될 때 마다, 커밋
        factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE

        return factory
    }

    /*
        auto.offset.reset
        #earliest : 마지막 커밋 기록이 없을 경우, 가장 예전(낮은 번호 오프셋) 레코드부터 처리
        #latest : 마지막 커밋 기록이 없을 경우, 가장 최근(높은 번호 오프셋) 레코드부터 처리
        #none : 커밋 기록이 없을 경우 throws Exception

        파티션의 개수가 변경될 경우 컨슈머는 metadata.max.age.ms 만큼 메타데이터 리프래시 기간 이후 리밸런싱이
        일어나면서 파티션 할당과정을 거치게 됩니다.
        문제는 메타데이터 리프레시(파티션 변경 여부를 알아차리는 시간) 기간동안 새로운 파티션에 데이터가 들어올 수 있다
        latest 로 설정하면 리밸런싱 중 들어온 신규데이터 부터 가져오기 때문에 리밸런싱 데이터는 누락되는 현상 발생
     */
    private fun getConfig(): Map<String, Any> =
        mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to BOOTSTRAP_SERVER,
            ConsumerConfig.GROUP_ID_CONFIG to CONSUMER_GROUP_ID,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java
        )

    @Bean
    fun coroutineScope(): CoroutineScope {
        return CoroutineScope(Dispatchers.Default + SupervisorJob())
    }
}