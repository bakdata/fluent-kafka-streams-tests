package com.bakdata.fluent_kafka_streams_tests.testutils;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.codehaus.jackson.map.annotate.JsonDeserialize;


@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonDeserialize(as = ClickOutput.class)
@JsonTypeInfo(use=JsonTypeInfo.Id.CLASS, property="@class")
public class ClickOutput {
    int userId;
    long count;
    long time;
}
