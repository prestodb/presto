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
package com.facebook.presto.pulsar;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.base.Preconditions.checkArgument;

public class PulsarRecordCursor implements RecordCursor {

    private List<PulsarColumnHandle> columnHandles;

    private static final Logger log = Logger.get(PulsarRecordCursor.class);


    public PulsarRecordCursor(List<PulsarColumnHandle> columnHandles) {
        this.columnHandles = columnHandles;

//        for (PulsarColumnHandle pulsarColumnHandle : this.columnHandles) {
//            for (int i = 0; i<10; i++) {
//                if (pulsarColumnHandle.getType() == IntegerType.INTEGER) {
//
//                }
//            }
//        }
    }

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public Type getType(int field) {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getType();
    }

    int counter = 0;
    int end = 10;
    @Override
    public boolean advanceNextPosition() {
        if (counter >= end) {
            return false;
        }
        counter++;
        return true;
    }

    @Override
    public boolean getBoolean(int field) {
        checkFieldType(field, boolean.class);

        return false;
    }

    @Override
    public long getLong(int field) {
        log.info("getLong: %s", field);
        checkFieldType(field, long.class);

        return counter;
    }

    @Override
    public double getDouble(int field) {
        checkFieldType(field, double.class);

        return counter;
    }

    @Override
    public Slice getSlice(int field) {
        checkFieldType(field, Slice.class);

        return Slices.utf8Slice("foo" + counter);
    }

    @Override
    public Object getObject(int field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field) {
        return false;
    }

    @Override
    public void close() {

    }

    private void checkFieldType(int field, Class<?> expected)
    {
        Class<?> actual = getType(field).getJavaType();
        checkArgument(actual == expected, "Expected field %s to be type %s but is %s", field, expected, actual);
    }
}
