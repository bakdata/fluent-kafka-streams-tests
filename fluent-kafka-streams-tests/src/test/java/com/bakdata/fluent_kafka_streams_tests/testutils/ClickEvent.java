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
    int userId;
    Integer status;

    public ClickEvent(final int userId) {
        this.userId = userId;
    }
}