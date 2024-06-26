description = "Mocks the HTTP endpoint of the schema registry for seamlessly testing topologies with Avro serdes"

dependencies {
    val confluentVersion: String by project
    "api"(group = "io.confluent", name = "kafka-avro-serializer", version = confluentVersion)
    "api"(group = "io.confluent", name = "kafka-schema-registry-client", version = confluentVersion)
    "api"(group = "io.confluent", name = "kafka-streams-avro-serde", version = confluentVersion)

    implementation(group = "org.wiremock", name = "wiremock", version = "3.4.2")
    // required because other dependencies use different Jackson versions if this library is used in test scope
    api(group = "com.fasterxml.jackson.core", name = "jackson-databind", version = "2.15.3")

    val junit5Version: String by project
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junit5Version)
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junit5Version)
    testImplementation(group = "io.confluent", name = "kafka-protobuf-provider", version = confluentVersion)
}
