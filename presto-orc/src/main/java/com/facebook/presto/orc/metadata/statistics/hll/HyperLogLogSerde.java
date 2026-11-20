/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.orc.metadata.statistics.hll;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Long.toHexString;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.stream.Collectors.toList;

/**
 * Serializes/deserializes a per-column list of HyperLogLog snapshots for storage
 * in the ORC footer user-metadata under {@link #HLL_USER_METADATA_KEY}.
 *
 * <p>Ported from bbio hive-dwrf ({@code com.facebook.hive.orc.hll.HyperLogLogSerde})
 * so the byte layout is identical to what the Spectrum stats publisher's
 * {@code HyperLogLogUtils} reads: little-endian MAGIC (8) | VERSION (2) |
 * positionCount (4) | isPresent[positionCount] (1 each) | length[positionCount]
 * (4 each) | concatenated present HLL payloads.
 */
public final class HyperLogLogSerde
{
    private HyperLogLogSerde() {}

    /** Footer user-metadata key that holds the serialized per-column HLL snapshots. */
    public static final String HLL_USER_METADATA_KEY = "__ORC_STREAMS_HLL_SNAPSHOTS__";

    /** Bucket count matching the legacy DWRF writer (and reader) expectations. */
    public static final int DEFAULT_NUMBER_OF_BUCKETS = 2048;

    private static final long MAGIC = 0xF67F9899EEED1BC9L;
    private static final short VERSION = 1;

    private static final byte TRUE = 1;
    private static final byte FALSE = 0;

    public static ByteBuffer serialize(List<Optional<HyperLogLog>> deserialized)
    {
        int positionCount = deserialized.size();

        List<Optional<Slice>> serializedHlls = deserialized.stream()
                .map(hll -> hll.map(HyperLogLog::serialize))
                .collect(toList());
        int payloadSize = serializedHlls.stream()
                .mapToInt(slice -> slice.map(Slice::length).orElse(0))
                .sum();

        int bufferSize = getBufferSize(positionCount, payloadSize);

        ByteBuffer output = ByteBuffer.allocate(bufferSize);
        output.order(LITTLE_ENDIAN);

        output.putLong(MAGIC);
        output.putShort(VERSION);
        output.putInt(positionCount);

        // write "isPresent" markers
        for (Optional<Slice> slice : serializedHlls) {
            output.put(slice.isPresent() ? TRUE : FALSE);
        }

        // write lengths
        for (Optional<Slice> slice : serializedHlls) {
            output.putInt(slice.map(Slice::length).orElse(0));
        }

        // write serialized hll's
        for (Optional<Slice> serializedHll : serializedHlls) {
            if (!serializedHll.isPresent()) {
                continue;
            }

            Slice slice = serializedHll.get();
            slice.getBytes(0, output.array(), output.position(), slice.length());
            output.position(output.position() + slice.length());
        }

        output.position(0);

        return output;
    }

    private static int getBufferSize(int positionCount, int payloadSize)
    {
        return Long.BYTES + // MAGIC
                Short.BYTES + // VERSION
                Integer.BYTES + // POSITION COUNT
                Byte.BYTES * positionCount + // IS PRESENT MARKERS
                Integer.BYTES * positionCount + // PAYLOAD LENGTHS
                payloadSize;
    }

    public static List<Optional<HyperLogLog>> deserialize(ByteBuffer buffer)
    {
        ByteOrder originalOrder = buffer.order();
        try {
            buffer.order(LITTLE_ENDIAN);

            long magic = buffer.getLong();
            checkArgument(magic == MAGIC, "Invalid magic bits: %s", toHexString(magic));
            short version = buffer.getShort();
            checkArgument(version == VERSION, "Unsupported version: %s. Supported versions: [%s]", version, VERSION);
            int positionsCount = buffer.getInt();
            checkArgument(positionsCount >= 0, "positionsCount must be greater than or equal to zero");

            boolean[] isPresent = new boolean[positionsCount];
            for (int i = 0; i < positionsCount; i++) {
                isPresent[i] = (buffer.get() == TRUE);
            }

            int maxLength = 0;
            int[] lengths = new int[positionsCount];
            for (int i = 0; i < positionsCount; i++) {
                int length = buffer.getInt();
                checkArgument(length >= 0, "length must be greater than or equal to zero");
                lengths[i] = length;
                if (length > maxLength) {
                    maxLength = length;
                }
            }

            byte[] sliceBuffer = new byte[maxLength];
            ImmutableList.Builder<Optional<HyperLogLog>> result = ImmutableList.builder();
            for (int i = 0; i < positionsCount; i++) {
                int length = lengths[i];
                if (!isPresent[i]) {
                    checkArgument(length == 0, "length must be zero for a non present element");
                    result.add(Optional.empty());
                    continue;
                }
                buffer.get(sliceBuffer, 0, length);
                HyperLogLog deserializedHll = HyperLogLog.newInstance(wrappedBuffer(sliceBuffer, 0, length));
                result.add(Optional.of(deserializedHll));
            }
            return result.build();
        }
        finally {
            buffer.order(originalOrder);
        }
    }
}
