description = "Mocks the HTTP endpoint of the schema registry for seamlessly testing topologies with Avro serdes"

dependencies {
    val junit5Version: String by project
    api(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junit5Version)
    api(project(":schema-registry-mock"))

    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junit5Version)
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junit5Version)
}
