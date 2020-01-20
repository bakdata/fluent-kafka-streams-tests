package com.bakdata.fluent_kafka_streams_tests.test_types;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonDeserialize(as = ErrorOutput.class)
@JsonTypeInfo(use=JsonTypeInfo.Id.CLASS, property="@class")
public class ErrorOutput {
    int statusCode;
    long count;
    long time;
    String definition;
}
