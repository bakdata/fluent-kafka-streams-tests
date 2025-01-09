description = "Provides the fluent Kafka Streams test framework."

dependencies {
    api(project(":fluent-kafka-streams-tests"))

    val junit4Version: String by project
    api(group = "junit", name = "junit", version = junit4Version)
    testImplementation(group = "junit", name = "junit", version = junit4Version)
}

tasks.test {
    useJUnit()
}
