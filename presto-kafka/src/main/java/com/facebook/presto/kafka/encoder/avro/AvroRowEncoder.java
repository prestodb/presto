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
package com.facebook.presto.kafka.encoder.avro;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.kafka.encoder.AbstractRowEncoder;
import com.facebook.presto.kafka.encoder.EncoderColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.google.common.collect.ImmutableSet;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AvroRowEncoder
        extends AbstractRowEncoder
{
    private static final Set<Type> SUPPORTED_PRIMITIVE_TYPES = ImmutableSet.of(
            BOOLEAN, INTEGER, BIGINT, DOUBLE, REAL);

    public static final String NAME = "avro";

    private final ByteArrayOutputStream byteArrayOutputStream;
    private final Schema parsedSchema;
    private final DataFileWriter<GenericRecord> dataFileWriter;
    private final GenericRecord record;

    public AvroRowEncoder(ConnectorSession session, List<EncoderColumnHandle> columnHandles, Schema parsedSchema)
    {
        super(session, columnHandles);
        for (EncoderColumnHandle columnHandle : this.columnHandles) {
            checkArgument(columnHandle.getFormatHint() == null, "Unexpected format hint '%s' defined for column '%s'", columnHandle.getFormatHint(), columnHandle.getName());
            checkArgument(columnHandle.getDataFormat() == null, "Unexpected data format '%s' defined for column '%s'", columnHandle.getDataFormat(), columnHandle.getName());

            checkArgument(isSupportedType(columnHandle.getType()), "Unsupported column type '%s' for column '%s'", columnHandle.getType(), columnHandle.getName());
        }
        this.byteArrayOutputStream = new ByteArrayOutputStream();
        this.parsedSchema = requireNonNull(parsedSchema, "parsedSchema is null");
        this.dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>(this.parsedSchema));
        this.record = new GenericData.Record(this.parsedSchema);
    }

    private boolean isSupportedType(Type type)
    {
        return isVarcharType(type) || SUPPORTED_PRIMITIVE_TYPES.contains(type);
    }

    @Override
    protected void appendNullValue()
    {
        record.put(columnHandles.get(currentColumnIndex).getName(), null);
    }

    @Override
    protected void appendLong(long value)
    {
        record.put(columnHandles.get(currentColumnIndex).getName(), value);
    }

    @Override
    protected void appendInt(int value)
    {
        record.put(columnHandles.get(currentColumnIndex).getName(), value);
    }

    @Override
    protected void appendShort(short value)
    {
        record.put(columnHandles.get(currentColumnIndex).getName(), value);
    }

    @Override
    protected void appendByte(byte value)
    {
        record.put(columnHandles.get(currentColumnIndex).getName(), value);
    }

    @Override
    protected void appendDouble(double value)
    {
        record.put(columnHandles.get(currentColumnIndex).getName(), value);
    }

    @Override
    protected void appendFloat(float value)
    {
        record.put(columnHandles.get(currentColumnIndex).getName(), value);
    }

    @Override
    protected void appendBoolean(boolean value)
    {
        record.put(columnHandles.get(currentColumnIndex).getName(), value);
    }

    @Override
    protected void appendString(String value)
    {
        record.put(columnHandles.get(currentColumnIndex).getName(), value);
    }

    @Override
    public byte[] toByteArray()
    {
        // make sure entire row has been updated with new values
        checkArgument(currentColumnIndex == columnHandles.size(), format("Missing %d columns", columnHandles.size() - currentColumnIndex + 1));

        try {
            byteArrayOutputStream.reset();
            dataFileWriter.create(parsedSchema, byteArrayOutputStream);
            dataFileWriter.append(record);
            dataFileWriter.close();

            resetColumnIndex(); // reset currentColumnIndex to prepare for next row
            return byteArrayOutputStream.toByteArray();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to append record", e);
        }
    }

    @Override
    public void close()
    {
        try {
            byteArrayOutputStream.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to close ByteArrayOutputStream", e);
        }
    }
}
