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
package com.facebook.plugin.arrow;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.google.common.base.CharMatcher;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.facebook.plugin.arrow.ArrowErrorCode.ARROW_FLIGHT_ERROR;

public class ArrowPageSource
        implements ConnectorPageSource
{
    private static final Logger logger = Logger.get(ArrowPageSource.class);
    private final ArrowSplit split;
    private final List<ArrowColumnHandle> columnHandles;
    private boolean completed;
    private int currentPosition;
    private Optional<VectorSchemaRoot> vectorSchemaRoot = Optional.empty();
    private ArrowFlightClient flightClient;
    private FlightStream flightStream;

    public ArrowPageSource(ArrowSplit split, List<ArrowColumnHandle> columnHandles, ArrowFlightClientHandler clientHandler,
            ConnectorSession connectorSession)
    {
        this.columnHandles = columnHandles;
        this.split = split;
        getFlightStream(clientHandler, split.getTicket(), connectorSession);
    }

    private void getFlightStream(ArrowFlightClientHandler clientHandler, byte[] ticket, ConnectorSession connectorSession)
    {
        try {
            Optional<String> uri = (split.getLocationUrls().isEmpty()) ?
                    Optional.empty() : Optional.of(split.getLocationUrls().get(0));
            flightClient = clientHandler.getClient(uri);
            flightStream = flightClient.getFlightClient().getStream(new Ticket(ticket), clientHandler.getCallOptions(connectorSession));
        }
        catch (FlightRuntimeException e) {
            throw new ArrowException(ARROW_FLIGHT_ERROR, e.getMessage(), e);
        }
    }

    private Block buildBlockFromVector(FieldVector vector, Type type)
    {
        if (vector instanceof BitVector) {
            return buildBlockFromBitVector((BitVector) vector, type);
        }
        else if (vector instanceof TinyIntVector) {
            return buildBlockFromTinyIntVector((TinyIntVector) vector, type);
        }
        else if (vector instanceof IntVector) {
            return buildBlockFromIntVector((IntVector) vector, type);
        }
        else if (vector instanceof SmallIntVector) {
            return buildBlockFromSmallIntVector((SmallIntVector) vector, type);
        }
        else if (vector instanceof BigIntVector) {
            return buildBlockFromBigIntVector((BigIntVector) vector, type);
        }
        else if (vector instanceof DecimalVector) {
            return buildBlockFromDecimalVector((DecimalVector) vector, type);
        }
        else if (vector instanceof NullVector) {
            return buildBlockFromNullVector((NullVector) vector, type);
        }
        else if (vector instanceof TimeStampMicroVector) {
            return buildBlockFromTimeStampMicroVector((TimeStampMicroVector) vector, type);
        }
        else if (vector instanceof TimeStampMilliVector) {
            return buildBlockFromTimeStampMilliVector((TimeStampMilliVector) vector, type);
        }
        else if (vector instanceof Float4Vector) {
            return buildBlockFromFloat4Vector((Float4Vector) vector, type);
        }
        else if (vector instanceof Float8Vector) {
            return buildBlockFromFloat8Vector((Float8Vector) vector, type);
        }
        else if (vector instanceof VarCharVector) {
            if (type instanceof CharType) {
                return buildCharTypeBlockFromVarcharVector((VarCharVector) vector, type);
            }
            else if (type instanceof TimeType) {
                return buildTimeTypeBlockFromVarcharVector((VarCharVector) vector, type);
            }
            else {
                return buildBlockFromVarCharVector((VarCharVector) vector, type);
            }
        }
        else if (vector instanceof VarBinaryVector) {
            return buildBlockFromVarBinaryVector((VarBinaryVector) vector, type);
        }
        else if (vector instanceof DateDayVector) {
            return buildBlockFromDateDayVector((DateDayVector) vector, type);
        }
        else if (vector instanceof DateMilliVector) {
            return buildBlockFromDateMilliVector((DateMilliVector) vector, type);
        }
        else if (vector instanceof TimeMilliVector) {
            return buildBlockFromTimeMilliVector((TimeMilliVector) vector, type);
        }
        else if (vector instanceof TimeSecVector) {
            return buildBlockFromTimeSecVector((TimeSecVector) vector, type);
        }
        else if (vector instanceof TimeStampSecVector) {
            return buildBlockFromTimeStampSecVector((TimeStampSecVector) vector, type);
        }
        else if (vector instanceof TimeMicroVector) {
            return buildBlockFromTimeMicroVector((TimeMicroVector) vector, type);
        }
        else if (vector instanceof TimeStampMilliTZVector) {
            return buildBlockFromTimeMilliTZVector((TimeStampMilliTZVector) vector, type);
        }
        throw new UnsupportedOperationException("Unsupported vector type: " + vector.getClass().getSimpleName());
    }

    private Block buildBlockFromTimeMilliTZVector(TimeStampMilliTZVector vector, Type type)
    {
        if (!(type instanceof TimestampType)) {
            throw new IllegalArgumentException("Type must be a TimestampType for TimeStampMilliTZVector");
        }

        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                long millis = vector.get(i);
                type.writeLong(builder, millis);
            }
        }
        return builder.build();
    }

    private Block buildBlockFromBitVector(BitVector vector, Type type)
    {
        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                type.writeBoolean(builder, vector.get(i) == 1);
            }
        }
        return builder.build();
    }

    private Block buildBlockFromIntVector(IntVector vector, Type type)
    {
        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                type.writeLong(builder, vector.get(i));
            }
        }
        return builder.build();
    }

    private Block buildBlockFromSmallIntVector(SmallIntVector vector, Type type)
    {
        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                type.writeLong(builder, vector.get(i));
            }
        }
        return builder.build();
    }

    private Block buildBlockFromTinyIntVector(TinyIntVector vector, Type type)
    {
        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                type.writeLong(builder, vector.get(i));
            }
        }
        return builder.build();
    }

    private Block buildBlockFromBigIntVector(BigIntVector vector, Type type)
    {
        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                type.writeLong(builder, vector.get(i));
            }
        }
        return builder.build();
    }

    private Block buildBlockFromDecimalVector(DecimalVector vector, Type type)
    {
        if (!(type instanceof DecimalType)) {
            throw new IllegalArgumentException("Type must be a DecimalType for DecimalVector");
        }

        DecimalType decimalType = (DecimalType) type;
        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());

        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                BigDecimal decimal = vector.getObject(i); // Get the BigDecimal value
                if (decimalType.isShort()) {
                    builder.writeLong(decimal.unscaledValue().longValue());
                }
                else {
                    Slice slice = Decimals.encodeScaledValue(decimal);
                    decimalType.writeSlice(builder, slice, 0, slice.length());
                }
            }
        }
        return builder.build();
    }

    private Block buildBlockFromNullVector(NullVector vector, Type type)
    {
        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            builder.appendNull();
        }
        return builder.build();
    }

    private Block buildBlockFromTimeStampMicroVector(TimeStampMicroVector vector, Type type)
    {
        if (!(type instanceof TimestampType)) {
            throw new IllegalArgumentException("Expected TimestampType but got " + type.getClass().getName());
        }

        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                long micros = vector.get(i);
                long millis = TimeUnit.MICROSECONDS.toMillis(micros);
                type.writeLong(builder, millis);
            }
        }
        return builder.build();
    }

    private Block buildBlockFromTimeStampMilliVector(TimeStampMilliVector vector, Type type)
    {
        if (!(type instanceof TimestampType)) {
            throw new IllegalArgumentException("Expected TimestampType but got " + type.getClass().getName());
        }

        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                long millis = vector.get(i);
                type.writeLong(builder, millis);
            }
        }
        return builder.build();
    }

    private Block buildBlockFromFloat8Vector(Float8Vector vector, Type type)
    {
        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                type.writeDouble(builder, vector.get(i));
            }
        }
        return builder.build();
    }

    private Block buildBlockFromFloat4Vector(Float4Vector vector, Type type)
    {
        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                int intBits = Float.floatToIntBits(vector.get(i));
                type.writeLong(builder, intBits);
            }
        }
        return builder.build();
    }

    private Block buildBlockFromVarBinaryVector(VarBinaryVector vector, Type type)
    {
        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                byte[] value = vector.get(i);
                type.writeSlice(builder, Slices.wrappedBuffer(value));
            }
        }
        return builder.build();
    }

    private Block buildBlockFromVarCharVector(VarCharVector vector, Type type)
    {
        if (!(type instanceof VarcharType)) {
            throw new IllegalArgumentException("Expected VarcharType but got " + type.getClass().getName());
        }

        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                String value = new String(vector.get(i), StandardCharsets.UTF_8);
                type.writeSlice(builder, Slices.utf8Slice(value));
            }
        }
        return builder.build();
    }

    private Block buildBlockFromDateDayVector(DateDayVector vector, Type type)
    {
        if (!(type instanceof DateType)) {
            throw new IllegalArgumentException("Expected DateType but got " + type.getClass().getName());
        }

        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                type.writeLong(builder, vector.get(i));
            }
        }
        return builder.build();
    }

    private Block buildBlockFromDateMilliVector(DateMilliVector vector, Type type)
    {
        if (!(type instanceof DateType)) {
            throw new IllegalArgumentException("Expected DateType but got " + type.getClass().getName());
        }

        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                DateType dateType = (DateType) type;
                long days = TimeUnit.MILLISECONDS.toDays(vector.get(i));
                dateType.writeLong(builder, days);
            }
        }
        return builder.build();
    }

    private Block buildBlockFromTimeSecVector(TimeSecVector vector, Type type)
    {
        if (!(type instanceof TimeType)) {
            throw new IllegalArgumentException("Type must be a TimeType for TimeSecVector");
        }

        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                int value = vector.get(i);
                long millis = TimeUnit.SECONDS.toMillis(value);
                type.writeLong(builder, millis);
            }
        }
        return builder.build();
    }

    private Block buildBlockFromTimeMilliVector(TimeMilliVector vector, Type type)
    {
        if (!(type instanceof TimeType)) {
            throw new IllegalArgumentException("Type must be a TimeType for TimeSecVector");
        }

        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                long millis = vector.get(i);
                type.writeLong(builder, millis);
            }
        }
        return builder.build();
    }

    private Block buildBlockFromTimeMicroVector(TimeMicroVector vector, Type type)
    {
        if (!(type instanceof TimeType)) {
            throw new IllegalArgumentException("Type must be a TimeType for TimemicroVector");
        }
        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                long value = vector.get(i);
                long micro = TimeUnit.MICROSECONDS.toMillis(value);
                type.writeLong(builder, micro);
            }
        }
        return builder.build();
    }

    private Block buildBlockFromTimeStampSecVector(TimeStampSecVector vector, Type type)
    {
        if (!(type instanceof TimestampType)) {
            throw new IllegalArgumentException("Type must be a TimestampType for TimeStampSecVector");
        }

        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                long value = vector.get(i);
                long millis = TimeUnit.SECONDS.toMillis(value);
                type.writeLong(builder, millis);
            }
        }
        return builder.build();
    }

    private Block buildCharTypeBlockFromVarcharVector(VarCharVector vector, Type type)
    {
        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                String value = new String(vector.get(i), StandardCharsets.UTF_8);
                type.writeSlice(builder, Slices.utf8Slice(CharMatcher.is(' ').trimTrailingFrom(value)));
            }
        }
        return builder.build();
    }

    private Block buildTimeTypeBlockFromVarcharVector(VarCharVector vector, Type type)
    {
        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                String timeString = new String(vector.get(i), StandardCharsets.UTF_8);
                LocalTime time = LocalTime.parse(timeString);
                long millis = Duration.between(LocalTime.MIN, time).toMillis();
                type.writeLong(builder, millis);
            }
        }
        return builder.build();
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getCompletedPositions()
    {
        return currentPosition;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return completed;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public Page getNextPage()
    {
        if (vectorSchemaRoot.isPresent()) {
            vectorSchemaRoot.get().close();
            vectorSchemaRoot = Optional.empty();
        }

        if (flightStream.next()) {
            vectorSchemaRoot = Optional.ofNullable(flightStream.getRoot());
        }

        if (!vectorSchemaRoot.isPresent()) {
            completed = true;
        }

        if (isFinished()) {
            return null;
        }

        currentPosition++;

        List<Block> blocks = new ArrayList<>();
        for (int columnIndex = 0; columnIndex < columnHandles.size(); columnIndex++) {
            FieldVector vector = vectorSchemaRoot.get().getVector(columnIndex);
            Type type = columnHandles.get(columnIndex).getColumnType();

            Block block = buildBlockFromVector(vector, type);
            blocks.add(block);
        }

        return new Page(vectorSchemaRoot.get().getRowCount(), blocks.toArray(new Block[0]));
    }

    @Override
    public void close()
    {
        if (vectorSchemaRoot.isPresent()) {
            vectorSchemaRoot.get().close();
            vectorSchemaRoot = Optional.empty();
        }
        if (flightStream != null) {
            try {
                flightStream.close();
            }
            catch (Exception e) {
                logger.error(e);
            }
        }
        try {
            if (flightClient != null) {
                flightClient.close();
                flightClient = null;
            }
        }
        catch (Exception ex) {
            logger.error("Failed to close the flight client: %s", ex.getMessage(), ex);
        }
    }
}
