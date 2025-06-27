plugins {
    alias(libs.plugins.avro)
    alias(libs.plugins.protobuf)
    java
    idea // required for protobuf support in intellij
}

description = "Provides the fluent Kafka Streams test framework."


val protobufVersion = libs.protobuf.get().version
dependencies {
    api(platform(libs.kafka.bom)) // Central repository requires this as a direct dependency to resolve versions
    api(libs.kafka.streams.utils)
    api(libs.kafka.clients)
    api(libs.kafka.streams)
    api(libs.kafka.streams.testUtils)
    implementation(libs.jool)

    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.avro)
    testImplementation(libs.kafka.streams.avro.serde) {
        exclude(group = "org.apache.kafka", module = "kafka-clients") // force usage of OSS kafka-clients
    }
    testImplementation(libs.kafka.streams.protobuf.serde) {
        exclude(group = "org.apache.kafka", module = "kafka-clients") // force usage of OSS kafka-clients
        exclude(group = "org.apache.kafka", module = "kafka-streams")
    }
    testImplementation(libs.protobuf)
}

protobuf {
    protoc {
        // The artifact spec for the Protobuf Compiler
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
}
