plugins {
    `java-library`
    // release
    id("net.researchgate.release") version "2.6.0"
    id("com.bakdata.sonar") version "1.0.1"
    id("com.bakdata.sonatype") version "1.0.1"
    id("org.hildan.github.changelog") version "0.8.0"
}

allprojects {
    group = "com.bakdata.${rootProject.name}"

    tasks.withType<Test> {
        maxParallelForks = 4
    }

    repositories {
        mavenCentral()
    }
}

configure<com.bakdata.gradle.SonatypeSettings> {
    developers {
        developer {
            name.set("Arvid Heise")
            id.set("AHeise")
        }
        developer {
            name.set("Lawrence Benson")
            id.set("lawben")
        }
    }
}

configure<org.hildan.github.changelog.plugin.GitHubChangelogExtension> {
    githubUser = "bakdata"
    futureVersionTag = findProperty("changelog.releaseVersion")?.toString()
    sinceTag = findProperty("changelog.sinceTag")?.toString()
}

subprojects {
    apply(plugin = "java-library")
    // build fails for java 11, let"s wait for a newer lombok version
    configure<JavaPluginConvention> {
        sourceCompatibility = org.gradle.api.JavaVersion.VERSION_11
        targetCompatibility = org.gradle.api.JavaVersion.VERSION_11
    }

    dependencies {
        val junitVersion = "5.3.0"
        implementation(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
        testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junitVersion)
        testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
        testImplementation(group = "org.assertj", name = "assertj-core", version = "3.11.1")

        compileOnly("org.projectlombok:lombok:1.18.6")
        annotationProcessor("org.projectlombok:lombok:1.18.6")
        testCompileOnly("org.projectlombok:lombok:1.18.6")
        testAnnotationProcessor("org.projectlombok:lombok:1.18.6")
    }
}
