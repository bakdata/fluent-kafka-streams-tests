buildscript {
    repositories {
        maven {
            url = uri("https://plugins.gradle.org/m2/")
        }
    }
    dependencies {
        classpath("com.commercehub.gradle.plugin:gradle-avro-plugin:0.16.0")
    }
}

description = "Provides the fluent Kafka Streams test framework."

apply(plugin = "com.commercehub.gradle.plugin.avro")

repositories {
    // jcenter()
    maven(url = "http://packages.confluent.io/maven/")
}

dependencies {
    val kafkaVersion = "2.0.0"
    "api"(group = "org.apache.kafka", name = "kafka-clients", version = kafkaVersion)
    "api"(group = "org.apache.kafka", name = "kafka-streams", version = kafkaVersion)
    "api"(group = "org.apache.kafka", name = "kafka-streams-test-utils", version = kafkaVersion)
    implementation(project(":schema-registry-mock"))

    testImplementation(group = "org.apache.avro", name = "avro", version = "1.8.2")
}