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
package com.facebook.presto.rcfile.text;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.rcfile.ColumnData;
import com.facebook.presto.rcfile.EncodeOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.DateTimePrinter;

public class TimestampEncoding
        implements TextColumnEncoding
{
    private static final DateTimeFormatter HIVE_TIMESTAMP_PARSER;
    private final DateTimeFormatter dateTimeFormatter;

    static {
        @SuppressWarnings("SpellCheckingInspection")
        DateTimeParser[] timestampWithoutTimeZoneParser = {
                DateTimeFormat.forPattern("yyyy-M-d").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s.SSS").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s.SSSSSSS").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s.SSSSSSSSS").getParser(),
        };
        @SuppressWarnings("SpellCheckingInspection")
        DateTimePrinter timestampWithoutTimeZonePrinter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS").getPrinter();
        HIVE_TIMESTAMP_PARSER = new DateTimeFormatterBuilder().append(timestampWithoutTimeZonePrinter, timestampWithoutTimeZoneParser).toFormatter().withZoneUTC();
    }

    private final Type type;
    private final Slice nullSequence;
    private final StringBuilder buffer = new StringBuilder();

    public TimestampEncoding(Type type, Slice nullSequence, DateTimeZone hiveStorageTimeZone)
    {
        this.type = type;
        this.nullSequence = nullSequence;
        this.dateTimeFormatter = HIVE_TIMESTAMP_PARSER.withZone(hiveStorageTimeZone);
    }

    @Override
    public void encodeColumn(Block block, SliceOutput output, EncodeOutput encodeOutput)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                output.writeBytes(nullSequence);
            }
            else {
                long millis = type.getLong(block, position);
                buffer.setLength(0);
                HIVE_TIMESTAMP_PARSER.printTo(buffer, millis);
                for (int index = 0; index < buffer.length(); index++) {
                    output.writeByte(buffer.charAt(index));
                }
            }
            encodeOutput.closeEntry();
        }
    }

    @Override
    public void encodeValueInto(int depth, Block block, int position, SliceOutput output)
    {
        long millis = type.getLong(block, position);
        buffer.setLength(0);
        HIVE_TIMESTAMP_PARSER.printTo(buffer, millis);
        for (int index = 0; index < buffer.length(); index++) {
            output.writeByte(buffer.charAt(index));
        }
    }

    @Override
    public Block decodeColumn(ColumnData columnData)
    {
        int size = columnData.rowCount();
        BlockBuilder builder = type.createBlockBuilder(null, size);

        Slice slice = columnData.getSlice();
        for (int i = 0; i < size; i++) {
            int offset = columnData.getOffset(i);
            int length = columnData.getLength(i);
            if (length == 0 || nullSequence.equals(0, nullSequence.length(), slice, offset, length)) {
                builder.appendNull();
            }
            else {
                type.writeLong(builder, parseTimestamp(slice, offset, length));
            }
        }
        return builder.build();
    }

    @Override
    public void decodeValueInto(int depth, BlockBuilder builder, Slice slice, int offset, int length)
    {
        long millis = parseTimestamp(slice, offset, length);
        type.writeLong(builder, millis);
    }

    private long parseTimestamp(Slice slice, int offset, int length)
    {
        //noinspection deprecation
        return HIVE_TIMESTAMP_PARSER.parseMillis(new String(slice.getBytes(offset, length), 0));
    }
}
