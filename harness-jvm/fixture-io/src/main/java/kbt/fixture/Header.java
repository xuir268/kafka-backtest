package kbt.fixture;

import java.nio.ByteBuffer;

/**
 * One Kafka record header: a key and a value, both raw bytes.
 * The buffers are read-only views into the fixture segment, not copies.
 */
public record Header(ByteBuffer key, ByteBuffer value) {
}
