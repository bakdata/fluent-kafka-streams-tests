description = "Provides the fluent Kafka Streams test framework."

dependencies {
    api(project(":fluent-kafka-streams-tests"))

    compileOnly(libs.junit4)
    testImplementation(libs.junit4)
}

tasks.test {
    useJUnit()
}
