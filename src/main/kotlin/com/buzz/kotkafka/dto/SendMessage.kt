package com.buzz.kotkafka.dto

data class SendMessage(
    val keyNum : Int,
    val sender :String,
    val data : String
)
