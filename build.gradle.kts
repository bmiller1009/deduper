/*
 * This file was generated by the Gradle 'init' task.
 *
 * This generated file contains a sample Kotlin application project to get you started.
 */

plugins {
    // Apply the Kotlin JVM plugin to add support for Kotlin on the JVM.
    id("org.jetbrains.kotlin.jvm").version("1.3.21")

    // Apply the application plugin to add support for building a CLI application.
    application
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
    implementation("org.apache.avro:avro:1.9.0")
    implementation("com.fasterxml.jackson.core:jackson-core:2.9.4")
    implementation("org.jgrapht", "jgrapht-core", "1.3.1")
    implementation("org.apache.logging.log4j",  "log4j-core",  "2.11.2")
    implementation("commons-cli", "commons-cli","1.4")
    implementation("com.github.h-thurow", "simple-jndi","0.18.1")
    implementation("commons-codec", "commons-codec","1.12")
    // Use the Kotlin test library.
    testImplementation("org.jetbrains.kotlin:kotlin-test")

    // Use the Kotlin JUnit integration.
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit")
}

application {
    // Define the main class for the application.
    mainClassName = "org.bradfordmiller.deduper.AppKt"
}