description = "Mocks the HTTP endpoint of the schema registry for seamlessly testing topologies with Avro serdes"

dependencies {
    val confluentVersion: String by project
    "api"(group = "io.confluent", name = "kafka-avro-serializer", version = confluentVersion)
    "api"(group = "io.confluent", name = "kafka-schema-registry-client", version = confluentVersion)
    "api"(group = "io.confluent", name = "kafka-streams-avro-serde", version = confluentVersion)

    implementation(group = "com.github.tomakehurst", name = "wiremock", version = "2.20.0")

    val junit5Version: String by project
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junit5Version)
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junit5Version)
  
    // resolve dependency issues between wiremock and schema registry
    val jacksonVersion = "2.9.6"
    implementation(group = "com.fasterxml.jackson.core", name = "jackson-core", version = jacksonVersion)
    implementation(group = "com.fasterxml.jackson.core", name = "jackson-annotations", version = jacksonVersion)
    implementation(group = "com.fasterxml.jackson.core", name = "jackson-databind", version = jacksonVersion)
}

configure<JavaPluginConvention> {
    sourceCompatibility = org.gradle.api.JavaVersion.VERSION_1_8
    targetCompatibility = org.gradle.api.JavaVersion.VERSION_1_8
}
