# Targets to be added as components land.
.PHONY: conformance native-test
conformance:
	@echo 'conformance runner not implemented yet'

native-test:
	cmake -S core-native -B /tmp/kbt-core-build
	cmake --build /tmp/kbt-core-build
	ctest --test-dir /tmp/kbt-core-build --output-on-failure
