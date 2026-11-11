// fixture-io: the fixture format reader/writer for the JVM side.
// Deliberately dependency-free at runtime: this jar is embedded in customer builds,
// so it must not force version changes on their Kafka or Flink dependencies.

plugins {
    `java-library`
}

group = "TODO.kafkabacktest"   // placeholder, set once the project name is chosen
version = "0.1.0-SNAPSHOT"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
    withSourcesJar()
    withJavadocJar()
}

repositories {
    mavenCentral()
}

dependencies {
    // No implementation or api dependencies by design.

    testImplementation(platform("org.junit:junit-bom:5.11.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
}
