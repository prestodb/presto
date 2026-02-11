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
package com.facebook.presto.delta.deletionvector;

import com.facebook.presto.spi.PrestoException;
import com.google.common.base.CharMatcher;
import io.delta.kernel.internal.deletionvectors.Base85Codec;
import io.delta.kernel.internal.deletionvectors.RoaringBitmapArray;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import static com.facebook.presto.delta.DeltaErrorCode.DELTA_BAD_DATA;
import static com.facebook.presto.delta.DeltaErrorCode.DELTA_PARQUET_SCHEMA_MISMATCH;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.delta.kernel.internal.deletionvectors.Base85Codec.decodeUUID;
import static java.lang.Math.toIntExact;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vector-format
public final class DeletionVectors
{
    private static final int PORTABLE_ROARING_BITMAP_MAGIC_NUMBER = 1681511377;

    private static final String UUID_MARKER = "u"; // relative path with random prefix on disk
    private static final String PATH_MARKER = "p"; // absolute path on disk
    private static final String INLINE_MARKER = "i"; // inline

    private static final CharMatcher ALPHANUMERIC = CharMatcher.inRange('A', 'Z').or(CharMatcher.inRange('a', 'z')).or(CharMatcher.inRange('0', '9')).precomputed();

    private DeletionVectors() {}

    public static RoaringBitmapArray readDeletionVectors(FileSystem fileSystem, String location, DeletionVectorEntry deletionVector, int maxDeletionVectorSize)
            throws IOException
    {
        switch (deletionVector.getStorageType()) {
            case UUID_MARKER:
                String canonizedLocation = location.endsWith("/") ? location : location + "/";
                Path mergedPath = new Path(canonizedLocation + toFileName(deletionVector.getPathOrInlineDv()));
                HadoopInputFile uuidInputFile = HadoopInputFile.fromPath(mergedPath, fileSystem.getConf());
                if (!fileSystem.exists(uuidInputFile.getPath())) {
                    throw new IllegalArgumentException("Unable to find 'u' type deletion vector by path: " + deletionVector.getPathOrInlineDv());
                }
                ByteBuffer uuidBuffer = readDeletionVector(uuidInputFile, deletionVector.getOffset().orElseThrow(), deletionVector.sizeInBytes(), maxDeletionVectorSize);
                return deserializeDeletionVectors(uuidBuffer);
            case PATH_MARKER:
                HadoopInputFile pathInputFile = HadoopInputFile.fromPath(new Path(deletionVector.getPathOrInlineDv()), fileSystem.getConf());
                if (!fileSystem.exists(pathInputFile.getPath())) {
                    throw new IllegalArgumentException("Unable to find 'p' type deletion vector by path: " + deletionVector.getPathOrInlineDv());
                }
                ByteBuffer pathBuffer = readDeletionVector(pathInputFile, deletionVector.getOffset().orElseThrow(), deletionVector.sizeInBytes(), maxDeletionVectorSize);
                return deserializeDeletionVectors(pathBuffer);
            case INLINE_MARKER:
                throw new PrestoException(DELTA_BAD_DATA, "Unsupported storage type for deletion vector: " + deletionVector.getStorageType());
        }
        throw new IllegalArgumentException("Unexpected storage type: " + deletionVector.getStorageType());
    }

    public static String toFileName(String pathOrInlineDv)
    {
        int randomPrefixLength = pathOrInlineDv.length() - Base85Codec.ENCODED_UUID_LENGTH;
        String randomPrefix = pathOrInlineDv.substring(0, randomPrefixLength);
        checkArgument(ALPHANUMERIC.matchesAllOf(randomPrefix), "Random prefix must be alphanumeric: %s", randomPrefix);
        String prefix = randomPrefix.isEmpty() ? "" : randomPrefix + "/";
        String encodedUuid = pathOrInlineDv.substring(randomPrefixLength);
        UUID uuid = decodeUUID(encodedUuid);
        return "%sdeletion_vector_%s.bin".formatted(prefix, uuid);
    }

    private static ByteBuffer readDeletionVector(HadoopInputFile inputFile, int offset, int expectedSize, int maxDeletionVectorSize)
            throws IOException
    {
        if (expectedSize < 0 || expectedSize > maxDeletionVectorSize) {
            throw new PrestoException(DELTA_BAD_DATA,
                "Deletion vector size greater than configured max size: " + expectedSize);
        }
        try (SeekableInputStream input = inputFile.newStream()) {
            byte[] data = new byte[expectedSize + SIZE_OF_INT + SIZE_OF_INT];
            input.seek(offset);
            input.readFully(data, 0, SIZE_OF_INT + expectedSize + SIZE_OF_INT);
            ByteBuffer buffer = ByteBuffer.wrap(data);
            int actualSize = buffer.getInt(0);
            if (actualSize != expectedSize) {
                throw new PrestoException(DELTA_PARQUET_SCHEMA_MISMATCH,
                        "The size of deletion vector %s expects %s but got %s"
                                .formatted(inputFile.getPath().toString(), expectedSize, actualSize));
            }
            int checksum = buffer.getInt(SIZE_OF_INT + expectedSize);
            if (calculateChecksum(buffer.array(), buffer.arrayOffset() + SIZE_OF_INT, expectedSize) != checksum) {
                throw new PrestoException(DELTA_PARQUET_SCHEMA_MISMATCH,
                        "Checksum mismatch for deletion vector: " + inputFile.getPath().toString());
            }
            return buffer.slice(SIZE_OF_INT, expectedSize).order(LITTLE_ENDIAN);
        }
    }

    private static int calculateChecksum(byte[] data, int offset, int length)
    {
        // Delta Lake allows integer overflow intentionally because it's fine from checksum perspective
        // https://github.com/delta-io/delta/blob/039a29abb4abc72ac5912651679233dc983398d6/spark/src/main/scala/org/apache/spark/sql/delta/storage/dv/DeletionVectorStore.scala#L115
        Checksum crc = new CRC32();
        crc.update(data, offset, length);
        return (int) crc.getValue();
    }

    private static RoaringBitmapArray deserializeDeletionVectors(ByteBuffer buffer)
            throws IOException
    {
        checkArgument(buffer.order() == LITTLE_ENDIAN, "Byte order must be little endian: %s", buffer.order());
        int magicNumber = buffer.getInt();
        if (magicNumber == PORTABLE_ROARING_BITMAP_MAGIC_NUMBER) {
            int size = toIntExact(buffer.getLong());
            RoaringBitmapArray bitmaps = new RoaringBitmapArray();
            for (int i = 0; i < size; i++) {
                int key = buffer.getInt();
                checkArgument(key >= 0, "key must not be negative: %s", key);

                RoaringBitmap bitmap = new RoaringBitmap();
                bitmap.deserialize(buffer);
                bitmap.stream().forEach(bitmaps::add);

                // there seems to be no better way to ask how many bytes bitmap.deserialize has read
                int consumedBytes = bitmap.serializedSizeInBytes();
                buffer.position(buffer.position() + consumedBytes);
            }
            return bitmaps;
        }
        throw new IllegalArgumentException("Unsupported magic number: " + magicNumber);
    }
}
