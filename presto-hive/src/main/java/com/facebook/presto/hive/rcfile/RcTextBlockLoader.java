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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Arrays;

import static com.facebook.presto.hive.HiveBooleanParser.isFalse;
import static com.facebook.presto.hive.HiveBooleanParser.isTrue;
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
import static com.facebook.presto.hive.HiveUtil.base64Decode;
import static com.facebook.presto.hive.HiveUtil.isStructuralType;
import static com.facebook.presto.hive.HiveUtil.parseHiveDate;
import static com.facebook.presto.hive.HiveUtil.parseHiveTimestamp;
import static com.facebook.presto.hive.NumberParser.parseDouble;
import static com.facebook.presto.hive.NumberParser.parseLong;
import static com.facebook.presto.hive.util.SerDeUtils.serializeObject;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.facebook.presto.spi.type.Varchars.truncateToLength;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.wrappedBooleanArray;
import static io.airlift.slice.Slices.wrappedDoubleArray;
import static io.airlift.slice.Slices.wrappedLongArray;

public class RcTextBlockLoader
        implements RcFileBlockLoader
{
    private final DateTimeZone hiveStorageTimeZone;

    public RcTextBlockLoader(DateTimeZone hiveStorageTimeZone)
    {
        this.hiveStorageTimeZone = hiveStorageTimeZone;
    }

    @Override
    public LazyBlockLoader<LazyFixedWidthBlock> fixedWidthBlockLoader(RcFileColumnsBatch batch, int fieldId, HiveType hiveType)
    {
        if (HIVE_BOOLEAN.equals(hiveType)) {
            return new LazyBooleanBlockLoader(batch, fieldId);
        }
        if (HIVE_BYTE.equals(hiveType) || HIVE_SHORT.equals(hiveType) || HIVE_INT.equals(hiveType) || HIVE_LONG.equals(hiveType)) {
            return new LazyLongBlockLoader(batch, fieldId);
        }
        if (HIVE_DATE.equals(hiveType)) {
            return new LazyDateBlockLoader(batch, fieldId);
        }
        if (HIVE_TIMESTAMP.equals(hiveType)) {
            return new LazyTimestampBlockLoader(batch, fieldId, hiveStorageTimeZone);
        }
        if (HIVE_FLOAT.equals(hiveType) || HIVE_DOUBLE.equals(hiveType)) {
            return new LazyDoubleBlockLoader(batch, fieldId);
        }
        throw new UnsupportedOperationException("Unsupported column type: " + hiveType);
    }

    @Override
    public LazyBlockLoader<LazySliceArrayBlock> variableWidthBlockLoader(RcFileColumnsBatch batch, int fieldId, HiveType hiveType, ObjectInspector fieldInspector, Type type)
    {
        if (HIVE_STRING.equals(hiveType)) {
            return new LazyStringBlockLoader(batch, fieldId, type);
        }
        if (HIVE_BINARY.equals(hiveType)) {
            return new LazyBinaryBlockLoader(batch, fieldId);
        }
        throw new UnsupportedOperationException("Unsupported column type: " + hiveType);
    }

    @Override
    public LazyBlockLoader structuralBlockLoader(RcFileColumnsBatch batch, int fieldId, HiveType hiveType, ObjectInspector fieldInspector, Type type)
    {
        checkArgument(isStructuralType(hiveType), "hiveType (" + hiveType + ") is not structuralType");
        return new LazyStructuralBlockLoader(type, batch, fieldId, fieldInspector);
    }

    private static boolean isNull(byte[] bytes, int start, int length)
    {
        return length == "\\N".length() && bytes[start] == '\\' && bytes[start + 1] == 'N';
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

                    byte[] bytes = writable.getData();
                    int start = writable.getStart();
                    int length = writable.getLength();
                    if (isTrue(bytes, start, length)) {
                        vector[i] = true;
                    }
                    else if (isFalse(bytes, start, length)) {
                        vector[i] = false;
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
                    if (length == 0 || isNull(bytes, start, length)) {
                        isNull[i] = true;
                    }
                    else {
                        vector[i] = parseLong(bytes, start, length);
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

                    byte[] bytes = writable.getData();
                    int start = writable.getStart();
                    int length = writable.getLength();
                    if (length == 0 || isNull(bytes, start, length)) {
                        isNull[i] = true;
                    }
                    else {
                        String value = new String(bytes, start, length);
                        vector[i] = parseHiveDate(value);
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
        private final DateTimeZone hiveStorageTimeZone;
        private boolean loaded;

        private LazyTimestampBlockLoader(RcFileColumnsBatch batch, int fieldId, DateTimeZone hiveStorageTimeZone)
        {
            this.batch = batch;
            this.fieldId = fieldId;
            this.hiveStorageTimeZone = hiveStorageTimeZone;
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
                    if (length == 0 || isNull(bytes, start, length)) {
                        isNull[i] = true;
                    }
                    else {
                        String value = new String(bytes, start, length);
                        vector[i] = parseHiveTimestamp(value, hiveStorageTimeZone);
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

                    byte[] bytes = writable.getData();
                    int start = writable.getStart();
                    int length = writable.getLength();
                    if (length == 0 || isNull(bytes, start, length)) {
                        isNull[i] = true;
                    }
                    else {
                        vector[i] = parseDouble(bytes, start, length);
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

    private static final class LazyStringBlockLoader
            implements LazyBlockLoader<LazySliceArrayBlock>
    {
        private final RcFileColumnsBatch batch;
        private final int fieldId;
        private final Type type;
        private boolean loaded;

        private LazyStringBlockLoader(RcFileColumnsBatch batch, int fieldId, Type type)
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

                    byte[] bytes = writable.getData();
                    int start = writable.getStart();
                    int length = writable.getLength();
                    if (!isNull(bytes, start, length)) {
                        Slice value = Slices.wrappedBuffer(Arrays.copyOfRange(bytes, start, start + length));
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

    private static final class LazyBinaryBlockLoader
            implements LazyBlockLoader<LazySliceArrayBlock>
    {
        private final RcFileColumnsBatch batch;
        private final int fieldId;
        private boolean loaded;

        private LazyBinaryBlockLoader(RcFileColumnsBatch batch, int fieldId)
        {
            this.batch = batch;
            this.fieldId = fieldId;
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

                    byte[] bytes = writable.getData();
                    int start = writable.getStart();
                    int length = writable.getLength();
                    if (!isNull(bytes, start, length)) {
                        // yes we end up with an extra copy here because the Base64 only handles whole arrays
                        byte[] data = Arrays.copyOfRange(bytes, start, start + length);
                        vector[i] = base64Decode(data);
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
        private final RcFileColumnsBatch batch;
        private final int fieldId;
        private final ObjectInspector fieldInspector;
        private boolean loaded;
        private final Type type;

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

                    byte[] bytes = writable.getData();
                    int start = writable.getStart();
                    int length = writable.getLength();
                    if (!isNull(bytes, start, length)) {
                        LazyObject<? extends ObjectInspector> lazyObject = LazyFactory.createLazyObject(fieldInspector);
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
}
