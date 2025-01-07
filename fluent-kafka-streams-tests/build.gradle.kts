plugins {
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
    id("com.google.protobuf") version "0.9.4"
    java
    idea // required for protobuf support in intellij
}

description = "Provides the fluent Kafka Streams test framework."


dependencies {
    val kafkaVersion: String by project
    "api"(group = "org.apache.kafka", name = "kafka-clients", version = kafkaVersion)
    "api"(group = "org.apache.kafka", name = "kafka-streams", version = kafkaVersion)
    "api"(group = "org.apache.kafka", name = "kafka-streams-test-utils", version = kafkaVersion)
    api(project(":schema-registry-mock"))

    val junit5Version: String by project
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junit5Version)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junit5Version)
    testImplementation(group = "org.apache.avro", name = "avro", version = "1.12.0")
    val confluentVersion: String by project
    testImplementation(group = "io.confluent", name = "kafka-protobuf-provider", version = confluentVersion)
    testImplementation(group = "io.confluent", name = "kafka-streams-protobuf-serde", version = confluentVersion)
    testImplementation(group = "com.google.protobuf", name = "protobuf-java", version = "3.25.5")
}

protobuf {
    protoc {
        // The artifact spec for the Protobuf Compiler
        artifact = "com.google.protobuf:protoc:3.25.5"
    }
}
