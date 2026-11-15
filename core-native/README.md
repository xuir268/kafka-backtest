# core-native (C++)
The first native slice exposes a C ABI for decoding one `.seg` partition file.
`kbt_segment_open` copies and validates the input; record and header slices
remain valid until `kbt_segment_close`. See [kbt_core.h](include/kbt_core.h)
and [fixture-format.md](../spec/fixture-format.md).

Build and test with `cmake -S core-native -B /tmp/kbt-core-build &&
cmake --build /tmp/kbt-core-build && ctest --test-dir /tmp/kbt-core-build
--output-on-failure` from the repository root. Cross-segment merge, diff,
Panama binding, and native replay remain future slices.
