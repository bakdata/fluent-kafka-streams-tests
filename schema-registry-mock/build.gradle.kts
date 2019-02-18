description = "Mocks the HTTP endpoint of the schema registry for seamlessly testing topologies with Avro serdes"

repositories {
    // jcenter()
    maven(url = "http://packages.confluent.io/maven/")
}

dependencies {
    val confluentVersion = "5.1.0"
    "api"(group = "io.confluent", name = "kafka-avro-serializer", version = confluentVersion)
    "api"(group = "io.confluent", name = "kafka-schema-registry-client", version = confluentVersion)
    "api"(group = "io.confluent", name = "kafka-streams-avro-serde", version = confluentVersion)

    implementation(group = "com.github.tomakehurst", name = "wiremock", version = "2.20.0")
}