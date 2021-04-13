import com.google.protobuf.gradle.*
import org.gradle.kotlin.dsl.provider.gradleKotlinDslOf

plugins {
    id("com.commercehub.gradle.plugin.avro") version "0.16.0"
    id("com.google.protobuf") version "0.8.15"
    java
    idea // required for protobuf support in intellij
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
    val confluentVersion: String by project
    testImplementation(group = "io.confluent", name = "kafka-protobuf-provider", version = confluentVersion)
    testImplementation(group= "io.confluent", name = "kafka-streams-protobuf-serde", version = confluentVersion)
    testImplementation(group = "com.google.protobuf", name = "protobuf-java", version = ":3.15.3")
}



protobuf {
    protoc {
        // The artifact spec for the Protobuf Compiler
        artifact = "com.google.protobuf:protoc:3.6.1"
    }
}
