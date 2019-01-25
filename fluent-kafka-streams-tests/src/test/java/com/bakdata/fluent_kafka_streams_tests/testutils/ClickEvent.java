package com.bakdata.fluent_kafka_streams_tests.testutils;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ClickEvent {
    String ip;
    Integer userId;
    Long timestamp;
    String request;
    Integer status;
    String bytes;
    String referrer;
    String agent;
}