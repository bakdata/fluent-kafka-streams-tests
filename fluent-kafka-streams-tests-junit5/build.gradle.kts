description = "Provides the fluent Kafka Streams test framework."

dependencies {
    api(project(":fluent-kafka-streams-tests"))

    testRuntimeOnly(libs.junit.platform.launcher)
    api(libs.junit.jupiter)
}
