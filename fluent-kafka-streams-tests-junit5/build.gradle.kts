description = "Provides the fluent Kafka Streams test framework."

dependencies {
    api(project(":fluent-kafka-streams-tests"))

    val junit5Version: String by project
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junit5Version)
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junit5Version)
    api(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junit5Version)
}
