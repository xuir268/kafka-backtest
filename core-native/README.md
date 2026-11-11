# core-native (C++)
Shared native core. Exposes a C ABI only. Used by the JVM harness via Panama FFM and by the native harness directly.
Stateless with respect to job semantics: bytes in, ordered record slices out.
