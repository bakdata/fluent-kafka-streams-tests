pluginManagement {
    repositories {
        gradlePluginPortal()
    }
}

dependencyResolutionManagement {
    repositoriesMode = RepositoriesMode.FAIL_ON_PROJECT_REPOS
    repositories {
        mavenCentral()
        maven(url = "https://packages.confluent.io/maven/")
        maven(url = "https://central.sonatype.com/repository/maven-snapshots")
    }
}

rootProject.name = "fluent-kafka-streams-tests"

listOf("", "-junit5", "-junit4").forEach { suffix ->
    include(":fluent-kafka-streams-tests$suffix")
}
