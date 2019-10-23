/*
 * This file was generated by the Gradle 'init' task.
 *
 * This generated file contains a sample Kotlin application project to get you started.
 */
import org.jetbrains.dokka.gradle.DokkaTask

plugins {
    // Apply the Kotlin JVM plugin to add support for Kotlin on the JVM.
    id("org.jetbrains.kotlin.jvm").version("1.3.50")
    id("org.jetbrains.dokka").version("0.10.0")
    id("net.researchgate.release").version("2.6.0")
    id("java-library")
    id("maven-publish")
}

//Sample gradle CLI: gradle release -Prelease.useAutomaticVersion=true
release {
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
    tagTemplate = "${version}"
    versionPropertyFile = "gradle.properties"
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
    implementation("org.postgresql", "postgresql", "42.2.8")
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

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = "org.bradfordmiller"
            artifactId = "deduper"
            version = "${version}"
            from(components["java"])
        }
    }

    repositories {
        maven {
            name = "deduper"
            url = uri("file://${buildDir}/repo")
        }
    }
}