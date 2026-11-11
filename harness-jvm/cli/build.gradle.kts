plugins {
    application
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
    implementation(project(":fixture-io"))
    implementation(project(":diff-engine"))
    implementation(project(":harness-core"))
    implementation(project(":harness-kafka-streams"))
    implementation("org.apache.kafka:kafka-clients:4.2.0")

    testImplementation(platform("org.junit:junit-bom:5.11.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    mainClass.set("kbt.cli.Main")
}

tasks.test {
    useJUnitPlatform()
}
