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
package com.facebook.presto.hive.rcfile;

import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.rcfile.RcFilePageSource.RcFileColumnsBatch;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.LazyArrayBlock;
import com.facebook.presto.spi.block.LazyBlockLoader;
import com.facebook.presto.spi.block.LazyFixedWidthBlock;
import com.facebook.presto.spi.block.LazySliceArrayBlock;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import io.airlift.slice.ByteArrays;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryFactory;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.WritableUtils;

import java.io.IOException;
import java.util.Arrays;

import static com.facebook.presto.hive.HiveType.HIVE_BINARY;
import static com.facebook.presto.hive.HiveType.HIVE_BOOLEAN;
import static com.facebook.presto.hive.HiveType.HIVE_BYTE;
import static com.facebook.presto.hive.HiveType.HIVE_DATE;
import static com.facebook.presto.hive.HiveType.HIVE_DOUBLE;
import static com.facebook.presto.hive.HiveType.HIVE_FLOAT;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.HiveType.HIVE_SHORT;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.HiveType.HIVE_TIMESTAMP;
import static com.facebook.presto.hive.HiveUtil.isStructuralType;
import static com.facebook.presto.hive.util.SerDeUtils.serializeObject;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.facebook.presto.spi.type.Varchars.truncateToLength;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static io.airlift.slice.Slices.wrappedBooleanArray;
import static io.airlift.slice.Slices.wrappedDoubleArray;
import static io.airlift.slice.Slices.wrappedLongArray;

public class RcBinaryBlockLoader
        implements RcFileBlockLoader
{
    private static final byte HIVE_EMPTY_STRING_BYTE = (byte) 0xbf;

    @Override
    public LazyBlockLoader<LazyFixedWidthBlock> fixedWidthBlockLoader(RcFileColumnsBatch batch, int fieldId, HiveType hiveType)
    {
        if (HIVE_BOOLEAN.equals(hiveType)) {
            return new LazyBooleanBlockLoader(batch, fieldId);
        }
        if (HIVE_BYTE.equals(hiveType)) {
            return new LazyByteBlockLoader(batch, fieldId);
        }
        if (HIVE_SHORT.equals(hiveType)) {
            return new LazyShortBlockLoader(batch, fieldId);
        }
        if (HIVE_INT.equals(hiveType)) {
            return new LazyIntBlockLoader(batch, fieldId);
        }
        if (HIVE_LONG.equals(hiveType)) {
            return new LazyLongBlockLoader(batch, fieldId);
        }
        if (HIVE_DATE.equals(hiveType)) {
            return new LazyDateBlockLoader(batch, fieldId);
        }
        if (HIVE_TIMESTAMP.equals(hiveType)) {
            return new LazyTimestampBlockLoader(batch, fieldId);
        }
        if (HIVE_FLOAT.equals(hiveType)) {
            return new LazyFloatBlockLoader(batch, fieldId);
        }
        if (HIVE_DOUBLE.equals(hiveType)) {
            return new LazyDoubleBlockLoader(batch, fieldId);
        }
        throw new UnsupportedOperationException("Unsupported column type: " + hiveType);
    }

    @Override
    public LazyBlockLoader<LazySliceArrayBlock> variableWidthBlockLoader(RcFileColumnsBatch batch, int fieldId, HiveType hiveType, ObjectInspector fieldInspector, Type type)
    {
        if (HIVE_STRING.equals(hiveType) || HIVE_BINARY.equals(hiveType)) {
            return new LazySliceBlockLoader(batch, fieldId, type);
        }
        throw new UnsupportedOperationException("Unsupported column type: " + hiveType);
    }

    @Override
    public LazyBlockLoader<LazyArrayBlock> structuralBlockLoader(RcFileColumnsBatch batch, int fieldId, HiveType hiveType, ObjectInspector fieldInspector, Type type)
    {
        checkArgument(isStructuralType(hiveType), "hiveType (" + hiveType + ") is not structuralType");
        return new LazyStructuralBlockLoader(type, batch, fieldId, fieldInspector);
    }

    private static final class LazyBooleanBlockLoader
            implements LazyBlockLoader<LazyFixedWidthBlock>
    {
        private final RcFileColumnsBatch batch;
        private final int fieldId;
        private boolean loaded;

        public LazyBooleanBlockLoader(RcFileColumnsBatch batch, int fieldId)
        {
            this.batch = batch;
            this.fieldId = fieldId;
        }

        @Override
        public void load(LazyFixedWidthBlock block)
        {
            if (loaded) {
                return;
            }

            try {
                BytesRefArrayWritable columnBatch = batch.getColumn(fieldId);
                int positionInBatch = batch.getPositionInBatch();

                int positionCount = block.getPositionCount();
                boolean[] isNull = new boolean[positionCount];
                boolean[] vector = new boolean[positionCount];

                for (int i = 0; i < positionCount; i++) {
                    BytesRefWritable writable = columnBatch.unCheckedGet(i + positionInBatch);

                    int length = writable.getLength();
                    if (length != 0) {
                        byte[] bytes = writable.getData();
                        int start = writable.getStart();
                        vector[i] = bytes[start] != 0;
                    }
                    else {
                        isNull[i] = true;
                    }
                }

                block.setNullVector(isNull);
                block.setRawSlice(wrappedBooleanArray(vector, 0, positionCount));

                loaded = true;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private static final class LazyByteBlockLoader
            implements LazyBlockLoader<LazyFixedWidthBlock>
    {
        private final RcFileColumnsBatch batch;
        private final int fieldId;
        private boolean loaded;

        private LazyByteBlockLoader(RcFileColumnsBatch batch, int fieldId)
        {
            this.batch = batch;
            this.fieldId = fieldId;
        }

        @Override
        public void load(LazyFixedWidthBlock block)
        {
            if (loaded) {
                return;
            }

            try {
                BytesRefArrayWritable columnBatch = batch.getColumn(fieldId);
                int positionInBatch = batch.getPositionInBatch();

                int batchSize = block.getPositionCount();
                boolean[] isNull = new boolean[batchSize];
                long[] vector = new long[batchSize];

                for (int i = 0; i < batchSize; i++) {
                    BytesRefWritable writable = columnBatch.unCheckedGet(i + positionInBatch);

                    int length = writable.getLength();
                    if (length != 0) {
                        byte[] bytes = writable.getData();
                        int start = writable.getStart();
                        vector[i] = bytes[start];
                    }
                    else {
                        isNull[i] = true;
                    }
                }

                block.setNullVector(isNull);
                block.setRawSlice(wrappedLongArray(vector));

                loaded = true;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private static final class LazyShortBlockLoader
            implements LazyBlockLoader<LazyFixedWidthBlock>
    {
        private final RcFileColumnsBatch batch;
        private final int fieldId;
        private boolean loaded;

        private LazyShortBlockLoader(RcFileColumnsBatch batch, int fieldId)
        {
            this.batch = batch;
            this.fieldId = fieldId;
        }

        @Override
        public void load(LazyFixedWidthBlock block)
        {
            if (loaded) {
                return;
            }

            try {
                BytesRefArrayWritable columnBatch = batch.getColumn(fieldId);
                int positionInBatch = batch.getPositionInBatch();

                int batchSize = block.getPositionCount();
                boolean[] isNull = new boolean[batchSize];
                long[] vector = new long[batchSize];

                for (int i = 0; i < batchSize; i++) {
                    BytesRefWritable writable = columnBatch.unCheckedGet(i + positionInBatch);

                    int length = writable.getLength();
                    if (length != 0) {
                        checkState(length == SIZE_OF_SHORT, "Short should be 2 bytes");

                        // the file format uses big endian
                        byte[] bytes = writable.getData();
                        int start = writable.getStart();
                        vector[i] = (long) Short.reverseBytes(ByteArrays.getShort(bytes, start));
                    }
                    else {
                        isNull[i] = true;
                    }
                }

                block.setNullVector(isNull);
                block.setRawSlice(wrappedLongArray(vector));

                loaded = true;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private static final class LazyIntBlockLoader
            implements LazyBlockLoader<LazyFixedWidthBlock>
    {
        private final RcFileColumnsBatch batch;
        private final int fieldId;
        private boolean loaded;

        private LazyIntBlockLoader(RcFileColumnsBatch batch, int fieldId)
        {
            this.batch = batch;
            this.fieldId = fieldId;
        }

        @Override
        public void load(LazyFixedWidthBlock block)
        {
            if (loaded) {
                return;
            }

            try {
                BytesRefArrayWritable columnBatch = batch.getColumn(fieldId);
                int positionInBatch = batch.getPositionInBatch();

                int batchSize = block.getPositionCount();
                boolean[] isNull = new boolean[batchSize];
                long[] vector = new long[batchSize];

                for (int i = 0; i < batchSize; i++) {
                    BytesRefWritable writable = columnBatch.unCheckedGet(i + positionInBatch);

                    byte[] bytes = writable.getData();
                    int start = writable.getStart();
                    int length = writable.getLength();
                    if (length == 0) {
                        isNull[i] = true;
                    }
                    else if (length == 1) {
                        vector[i] = bytes[start];
                    }
                    else {
                        vector[i] = readVInt(bytes, start, length);
                    }
                }

                block.setNullVector(isNull);
                block.setRawSlice(wrappedLongArray(vector));

                loaded = true;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private static final class LazyLongBlockLoader
            implements LazyBlockLoader<LazyFixedWidthBlock>
    {
        private final RcFileColumnsBatch batch;
        private final int fieldId;
        private boolean loaded;

        private LazyLongBlockLoader(RcFileColumnsBatch batch, int fieldId)
        {
            this.batch = batch;
            this.fieldId = fieldId;
        }

        @Override
        public void load(LazyFixedWidthBlock block)
        {
            if (loaded) {
                return;
            }

            try {
                BytesRefArrayWritable columnBatch = batch.getColumn(fieldId);
                int positionInBatch = batch.getPositionInBatch();

                int batchSize = block.getPositionCount();
                boolean[] isNull = new boolean[batchSize];
                long[] vector = new long[batchSize];

                for (int i = 0; i < batchSize; i++) {
                    BytesRefWritable writable = columnBatch.unCheckedGet(i + positionInBatch);

                    byte[] bytes = writable.getData();
                    int start = writable.getStart();
                    int length = writable.getLength();
                    if (length == 0) {
                        isNull[i] = true;
                    }
                    else if (length == 1) {
                        vector[i] = bytes[start];
                    }
                    else {
                        vector[i] = readVInt(bytes, start, length);
                    }
                }

                block.setNullVector(isNull);
                block.setRawSlice(wrappedLongArray(vector));

                loaded = true;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private static final class LazyDateBlockLoader
            implements LazyBlockLoader<LazyFixedWidthBlock>
    {
        private final RcFileColumnsBatch batch;
        private final int fieldId;
        private boolean loaded;

        private LazyDateBlockLoader(RcFileColumnsBatch batch, int fieldId)
        {
            this.batch = batch;
            this.fieldId = fieldId;
        }

        @Override
        public void load(LazyFixedWidthBlock block)
        {
            if (loaded) {
                return;
            }

            try {
                BytesRefArrayWritable columnBatch = batch.getColumn(fieldId);
                int positionInBatch = batch.getPositionInBatch();

                int positionCount = block.getPositionCount();
                boolean[] isNull = new boolean[positionCount];
                long[] vector = new long[positionCount];

                for (int i = 0; i < positionCount; i++) {
                    BytesRefWritable writable = columnBatch.unCheckedGet(i + positionInBatch);

                    int length = writable.getLength();
                    if (length != 0) {
                        byte[] bytes = writable.getData();
                        int start = writable.getStart();
                        long daysSinceEpoch = readVInt(bytes, start, length);
                        vector[i] = daysSinceEpoch;
                    }
                    else {
                        isNull[i] = true;
                    }
                }

                block.setNullVector(isNull);
                block.setRawSlice(wrappedLongArray(vector));

                loaded = true;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private static final class LazyTimestampBlockLoader
            implements LazyBlockLoader<LazyFixedWidthBlock>
    {
        private final RcFileColumnsBatch batch;
        private final int fieldId;
        private boolean loaded;

        private LazyTimestampBlockLoader(RcFileColumnsBatch batch, int fieldId)
        {
            this.batch = batch;
            this.fieldId = fieldId;
        }

        @Override
        public void load(LazyFixedWidthBlock block)
        {
            if (loaded) {
                return;
            }

            try {
                BytesRefArrayWritable columnBatch = batch.getColumn(fieldId);
                int positionInBatch = batch.getPositionInBatch();

                int batchSize = block.getPositionCount();
                boolean[] isNull = new boolean[batchSize];
                long[] vector = new long[batchSize];

                for (int i = 0; i < batchSize; i++) {
                    BytesRefWritable writable = columnBatch.unCheckedGet(i + positionInBatch);

                    int length = writable.getLength();
                    if (length != 0) {
                        byte[] bytes = writable.getData();
                        int start = writable.getStart();

                        long seconds = TimestampWritable.getSeconds(bytes, start);
                        long nanos = TimestampWritable.getNanos(bytes, start + SIZE_OF_INT);
                        vector[i] = (seconds * 1000) + (nanos / 1_000_000);
                    }
                    else {
                        isNull[i] = true;
                    }
                }

                block.setNullVector(isNull);
                block.setRawSlice(wrappedLongArray(vector));

                loaded = true;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private static final class LazyFloatBlockLoader
            implements LazyBlockLoader<LazyFixedWidthBlock>
    {
        private final RcFileColumnsBatch batch;
        private final int fieldId;
        private boolean loaded;

        private LazyFloatBlockLoader(RcFileColumnsBatch batch, int fieldId)
        {
            this.batch = batch;
            this.fieldId = fieldId;
        }

        @Override
        public void load(LazyFixedWidthBlock block)
        {
            if (loaded) {
                return;
            }

            try {
                BytesRefArrayWritable columnBatch = batch.getColumn(fieldId);
                int positionInBatch = batch.getPositionInBatch();

                int batchSize = block.getPositionCount();
                boolean[] isNull = new boolean[batchSize];
                double[] vector = new double[batchSize];

                for (int i = 0; i < batchSize; i++) {
                    BytesRefWritable writable = columnBatch.unCheckedGet(i + positionInBatch);

                    int length = writable.getLength();
                    if (length != 0) {
                        checkState(length == SIZE_OF_INT, "Float should be 4 bytes");

                        byte[] bytes = writable.getData();
                        int start = writable.getStart();
                        int intBits = ByteArrays.getInt(bytes, start);

                        // the file format uses big endian
                        vector[i] = (double) Float.intBitsToFloat(Integer.reverseBytes(intBits));
                    }
                    else {
                        isNull[i] = true;
                    }
                }

                block.setNullVector(isNull);
                block.setRawSlice(wrappedDoubleArray(vector));

                loaded = true;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private static final class LazyDoubleBlockLoader
            implements LazyBlockLoader<LazyFixedWidthBlock>
    {
        private final RcFileColumnsBatch batch;
        private final int fieldId;
        private boolean loaded;

        private LazyDoubleBlockLoader(RcFileColumnsBatch batch, int fieldId)
        {
            this.batch = batch;
            this.fieldId = fieldId;
        }

        @Override
        public void load(LazyFixedWidthBlock block)
        {
            if (loaded) {
                return;
            }

            try {
                BytesRefArrayWritable columnBatch = batch.getColumn(fieldId);
                int positionInBatch = batch.getPositionInBatch();

                int batchSize = block.getPositionCount();
                boolean[] isNull = new boolean[batchSize];
                double[] vector = new double[batchSize];

                for (int i = 0; i < batchSize; i++) {
                    BytesRefWritable writable = columnBatch.unCheckedGet(i + positionInBatch);

                    int length = writable.getLength();
                    if (length != 0) {
                        checkState(length == SIZE_OF_LONG, "Double should be 8 bytes");

                        byte[] bytes = writable.getData();
                        int start = writable.getStart();
                        long longBits = ByteArrays.getLong(bytes, start);

                        // the file format uses big endian
                        vector[i] = Double.longBitsToDouble(Long.reverseBytes(longBits));
                    }
                    else {
                        isNull[i] = true;
                    }
                }

                block.setNullVector(isNull);
                block.setRawSlice(wrappedDoubleArray(vector));

                loaded = true;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private static final class LazySliceBlockLoader
            implements LazyBlockLoader<LazySliceArrayBlock>
    {
        private final RcFileColumnsBatch batch;
        private final int fieldId;
        private final Type type;
        private boolean loaded;

        private LazySliceBlockLoader(RcFileColumnsBatch batch, int fieldId, Type type)
        {
            this.batch = batch;
            this.fieldId = fieldId;
            this.type = type;
        }

        @Override
        public void load(LazySliceArrayBlock block)
        {
            if (loaded) {
                return;
            }

            try {
                BytesRefArrayWritable columnBatch = batch.getColumn(fieldId);
                int positionInBatch = batch.getPositionInBatch();

                int batchSize = block.getPositionCount();
                Slice[] vector = new Slice[batchSize];

                for (int i = 0; i < batchSize; i++) {
                    BytesRefWritable writable = columnBatch.unCheckedGet(i + positionInBatch);

                    int length = writable.getLength();
                    if (length > 0) {
                        byte[] bytes = writable.getData();
                        int start = writable.getStart();
                        Slice value;
                        if ((length == 1) && bytes[start] == HIVE_EMPTY_STRING_BYTE) {
                            value = Slices.EMPTY_SLICE;
                        }
                        else {
                            value = Slices.wrappedBuffer(Arrays.copyOfRange(bytes, start, start + length));
                        }
                        if (isVarcharType(type)) {
                            value = truncateToLength(value, type);
                        }
                        vector[i] = value;
                    }
                }

                block.setValues(vector);

                loaded = true;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private static final class LazyStructuralBlockLoader
            implements LazyBlockLoader<LazyArrayBlock>
    {
        private final Type type;
        private final RcFileColumnsBatch batch;
        private final int fieldId;
        private final ObjectInspector fieldInspector;
        private boolean loaded;

        private LazyStructuralBlockLoader(Type type, RcFileColumnsBatch batch, int fieldId, ObjectInspector fieldInspector)
        {
            this.type = type;
            this.batch = batch;
            this.fieldId = fieldId;
            this.fieldInspector = fieldInspector;
        }

        @Override
        public void load(LazyArrayBlock block)
        {
            if (loaded) {
                return;
            }

            try {
                BytesRefArrayWritable columnBatch = batch.getColumn(fieldId);
                int positionInBatch = batch.getPositionInBatch();

                int batchSize = block.getPositionCount();
                BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), batchSize);

                for (int i = 0; i < batchSize; i++) {
                    BytesRefWritable writable = columnBatch.unCheckedGet(i + positionInBatch);

                    int length = writable.getLength();
                    if (length > 0) {
                        byte[] bytes = writable.getData();
                        int start = writable.getStart();
                        LazyBinaryObject lazyObject = LazyBinaryFactory.createLazyBinaryObject(fieldInspector);
                        ByteArrayRef byteArrayRef = new ByteArrayRef();
                        byteArrayRef.setData(bytes);
                        lazyObject.init(byteArrayRef, start, length);
                        serializeObject(type, blockBuilder, lazyObject.getObject(), fieldInspector);
                    }
                }

                block.copyFromBlock(blockBuilder.build());

                loaded = true;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    // faster version of org.apache.hadoop.io.WritableUtils.readVLong
    private static long readVInt(byte[] bytes, int start, int length)
    {
        long value = 0;
        for (int i = 1; i < length; i++) {
            value <<= 8;
            value |= (bytes[start + i] & 0xFF);
        }
        return WritableUtils.isNegativeVInt(bytes[start]) ? ~value : value;
    }
}
