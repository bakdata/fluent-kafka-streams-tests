description = "Mocks the HTTP endpoint of the schema registry for seamlessly testing topologies with Avro serdes"

dependencies {
    val junit4Version: String by project
    api(group = "junit", name = "junit", version = junit4Version)
    api(project(":schema-registry-mock"))

    testImplementation(group = "junit", name = "junit", version = junit4Version)
}

tasks.test {
    useJUnit()
}
