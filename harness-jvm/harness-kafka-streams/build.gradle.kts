plugins {
    `java-library`
}

group = "TODO.kafkabacktest"
version = "0.1.0-SNAPSHOT"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

repositories {
    mavenCentral()
}

dependencies {
    api(project(":harness-core"))
    api("org.apache.kafka:kafka-streams:4.2.0")
    implementation("org.apache.kafka:kafka-streams-test-utils:4.2.0")

    testImplementation(platform("org.junit:junit-bom:5.11.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
}
