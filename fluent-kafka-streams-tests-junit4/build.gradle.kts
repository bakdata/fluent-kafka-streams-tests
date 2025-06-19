description = "Provides the fluent Kafka Streams test framework."

dependencies {
    api(project(":fluent-kafka-streams-tests"))

    api(libs.junit)
}

tasks.test {
    useJUnit()
}
