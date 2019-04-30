description = "Mocks the HTTP endpoint of the schema registry for seamlessly testing topologies with Avro serdes"

repositories {
    // jcenter()
    maven(url = "http://packages.confluent.io/maven/")
}

dependencies {
    val confluentVersion = "5.1.2"
    "api"(group = "io.confluent", name = "kafka-avro-serializer", version = confluentVersion)
    "api"(group = "io.confluent", name = "kafka-schema-registry-client", version = confluentVersion)
    "api"(group = "io.confluent", name = "kafka-streams-avro-serde", version = confluentVersion)

    implementation(group = "com.github.tomakehurst", name = "wiremock", version = "2.20.0")

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
