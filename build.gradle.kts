plugins {
    // release
    id("com.bakdata.release") version "1.7.1"
    id("com.bakdata.sonar") version "1.7.1"
    id("com.bakdata.sonatype") version "1.9.0"
    id("io.freefair.lombok") version "8.12.2.1" apply false
}

allprojects {
    group = "com.bakdata.${rootProject.name}"

    tasks.withType<Test> {
        maxParallelForks = 4
    }

    repositories {
        mavenCentral()
        maven(url = "https://packages.confluent.io/maven/")
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
        developer {
            name.set("Torben Meyer")
            id.set("torbsto")
        }
        developer {
            name.set("Philipp Schirmer")
            id.set("philipp98431")
        }
    }
}

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "io.freefair.lombok")
    configure<JavaPluginExtension> {
        toolchain {
            languageVersion = JavaLanguageVersion.of(11)
        }
    }

    tasks.withType<Javadoc> {
        options {
            (this as StandardJavadocDocletOptions).apply {
                addBooleanOption("html5", true)
                stylesheetFile(File("$rootDir/src/main/javadoc/assertj-javadoc.css"))
                addBooleanOption("-allow-script-in-comments", true)
                header("<script src=\"http://cdn.jsdelivr.net/highlight.js/8.6/highlight.min.js\"></script>")
                footer("<script type=\"text/javascript\">hljs.initHighlightingOnLoad();</script>")
            }
        }
    }

    dependencies {
        val log4jVersion: String by project
        "testImplementation"(group = "org.apache.logging.log4j", name = "log4j-slf4j2-impl", version = log4jVersion)
        "testImplementation"(group = "org.assertj", name = "assertj-core", version = "3.27.2")
    }
}
