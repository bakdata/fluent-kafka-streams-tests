pluginManagement {
    repositories {
        gradlePluginPortal()
    }
}

rootProject.name = "fluent-kafka-streams-tests"

listOf("", "-junit5", "-junit4").forEach { suffix ->
    include(":fluent-kafka-streams-tests$suffix")
}
