plugins {
    // kotlin stuff
    `kotlin-dsl`
    // release
    id("net.researchgate.release") version "2.6.0"
    // eat your own dog food - apply the plugins to this plugin project
    id("com.bakdata.sonar") version "1.0.0"
    id("com.bakdata.sonatype") version "1.0.0"
    id("io.franzbecker.gradle-lombok") version "1.14"
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

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "io.franzbecker.gradle-lombok")

    lombok {
        version = "1.18.4"
        sha256 = ""
    }

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
    }
}
