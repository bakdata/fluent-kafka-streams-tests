plugins {
    // release
    id("net.researchgate.release") version "2.8.1"
    id("com.bakdata.sonar") version "1.1.7"
    id("com.bakdata.sonatype") version "1.1.7"
    id("org.hildan.github.changelog") version "0.8.0"
    id("io.freefair.lombok") version "5.3.3.3" apply false
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

configure<org.hildan.github.changelog.plugin.GitHubChangelogExtension> {
    githubUser = "bakdata"
    futureVersionTag = findProperty("changelog.releaseVersion")?.toString()
    sinceTag = findProperty("changelog.sinceTag")?.toString()
}

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "io.freefair.lombok")
    // build fails for java 11, let"s wait for a newer lombok version
    configure<JavaPluginConvention> {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
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
        "testImplementation"(group = "org.apache.logging.log4j", name = "log4j-slf4j-impl", version = log4jVersion)
        "testImplementation"(group = "org.assertj", name = "assertj-core", version = "3.20.2")
    }
}
