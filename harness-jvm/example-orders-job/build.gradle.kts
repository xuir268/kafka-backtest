plugins { `java-library` }

java { toolchain { languageVersion.set(JavaLanguageVersion.of(21)) } }
repositories { mavenCentral() }
dependencies {
    compileOnly(project(":harness-kafka-streams"))
    compileOnly("org.apache.kafka:kafka-streams:4.2.0")
}
