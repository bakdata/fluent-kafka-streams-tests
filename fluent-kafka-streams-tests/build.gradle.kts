plugins {
    id("com.commercehub.gradle.plugin.avro") version "0.16.0"
}

description = "Provides the fluent Kafka Streams test framework."

dependencies {
    val kafkaVersion: String by project
    "api"(group = "org.apache.kafka", name = "kafka-clients", version = kafkaVersion)
    "api"(group = "org.apache.kafka", name = "kafka-streams", version = kafkaVersion)
    "api"(group = "org.apache.kafka", name = "kafka-streams-test-utils", version = kafkaVersion)
    implementation(project(":schema-registry-mock"))

    val junit5Version: String by project
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junit5Version)
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junit5Version)
    testImplementation(group = "org.apache.avro", name = "avro", version = "1.8.2")
}
