/*
 * This file was generated by the Gradle 'init' task.
 *
 * This generated file contains a sample Kotlin application project to get you started.
 */
import org.jetbrains.dokka.gradle.DokkaTask
import groovy.lang.Closure
import org.jetbrains.kotlin.gradle.utils.loadPropertyFromResources
import java.util.Properties

plugins {
    // Apply the Kotlin JVM plugin to add support for Kotlin on the JVM.
    id("org.jetbrains.kotlin.jvm").version("1.3.50")
    id("org.jetbrains.dokka").version("0.10.0")
    id("net.researchgate.release").version("2.6.0")
    id("java-library")
    id("com.bmuschko.nexus").version("2.3.1")
    id("io.codearte.nexus-staging").version("0.21.1")
    id("de.marcphilipp.nexus-publish").version("0.3.0")
}

group = "org.bradfordmiller"

val props = Properties()
val inputStream = file("version.properties").inputStream()
props.load(inputStream)
val softwareVersion = properties.get("version")!!.toString()

tasks.build {
    dependsOn("set-defaults")
}

//Sample gradle CLI: gradle release -Prelease.useAutomaticVersion=true
release {

    val props = Properties()
    val inputStream = file("version.properties").inputStream()
    props.load(inputStream)
    val softwareVersion = properties.get("version")!!.toString()

    failOnCommitNeeded = true
    failOnPublishNeeded = true
    failOnSnapshotDependencies = true
    failOnUnversionedFiles = true
    failOnUpdateNeeded = true
    revertOnFail = true
    preCommitText = ""
    preTagCommitMessage = "[Gradle Release Plugin] - pre tag commit: "
    tagCommitMessage = "[Gradle Release Plugin] - creating tag: "
    newVersionCommitMessage = "[Gradle Release Plugin] - new version commit: "
    tagTemplate = softwareVersion
    versionPropertyFile = "version.properties"

    inputStream.close()
}

repositories {
    // Use jcenter for resolving your dependencies.
    // You can declare any Maven/Ivy/file repository here.
    jcenter()
}

dependencies {
    // Use the Kotlin JDK 8 standard library.
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation("net.sourceforge.csvjdbc:csvjdbc:1.0.35")
    implementation("com.fasterxml.jackson.core:jackson-core:2.9.4")
    implementation("org.apache.logging.log4j",  "log4j-core",  "2.12.0")
    implementation("org.apache.logging.log4j",  "log4j-api",  "2.12.0")
    implementation("com.github.h-thurow", "simple-jndi","0.18.1")
    implementation("commons-codec", "commons-codec","1.12")
    implementation("org.apache.logging.log4j", "log4j-slf4j-impl", "2.12.0")
    implementation("org.xerial", "sqlite-jdbc", "3.28.0")
    implementation("org.json", "json", "20190722")
    implementation("org.apache.commons", "commons-dbcp2", "2.7.0")
    implementation("commons-io", "commons-io", "2.6")
    implementation("net.sf.trove4j", "core", "3.1.0")
    implementation("com.opencsv", "opencsv", "4.6")
    // Use the Kotlin test library.
    testImplementation("org.jetbrains.kotlin:kotlin-test")

    // Use the Kotlin JUnit integration.
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:5.5.1")
}

tasks {

    val dokka by getting(DokkaTask::class) {
        outputFormat = "html"
        outputDirectory = "$buildDir/dokka"
    }
}

project.publishing.publications.withType(MavenPublication::class.java).forEach { publication ->
    with(publication.pom) {
        withXml {
            val root = asNode()
            root.appendNode("name", "deduper")
            root.appendNode("description", "General deduping engine for JDBC sources with output to JDBC/csv targets")
            root.appendNode("url", "https://github.com/bmiller1009/deduper")
        }

        licenses {
            license {
                name.set("The Apache Software License, Version 2.0")
                url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                distribution.set("repo")
            }
        }
        developers {
            developer {
                id.set("bmiller1009")
                name.set("Bradford Miller")
                email.set("bfm@bradfordmiller.org")
            }
        }
        scm {
            url.set("git@github.com:bmiller1009/deduper.git/")
            connection.set("scm:git@github.com:bmiller1009/deduper.git")
        }
    }
}




//val modifyPom : Closure<MavenPom> by ext

/*modifyPom(closureOf<MavenPom>{
    project {
        withGroovyBuilder {
            "name"("deduper")
            "description"("General deduping engine for JDBC sources with output to JDBC/csv targets")
            "url"("https://github.com/bmiller1009/deduper")
            "inceptionYear"("2019")

            "scm" {
                "url"("git@github.com:bmiller1009/deduper.git/")
                "connection"("scm:git@github.com:bmiller1009/deduper.git")
            }

            "licenses" {
                "license" {
                    "name"("The Apache Software License, Version 2.0")
                    "url"("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    "distribution"("repo")
                }
            }

            "developers" {
                "developer" {
                    "id" ("bmiller1009")
                    "name"("Bradford Miller")
                    "email"("bfm@bradfordmiller.org")
                }
            }
        }
    }
})*/

extraArchive {
    sources = true
    tests = true
    javadoc = true
}

nexus {
    sign = true
    repositoryUrl = "https://oss.sonatype.org/service/local/staging/deploy/maven2/"
    snapshotRepositoryUrl = "https://oss.sonatype.org/content/repositories/snapshots/"
}

nexusStaging {
    packageGroup = "org.bradfordmiller" //optional if packageGroup == project.getGroup()
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
        }
    }
}

val uname: String? by project
val pwd: String? by project

nexusPublishing {
    repositories {
            sonatype {
                username.set(uname)
                password.set(pwd)
            }
    }
}