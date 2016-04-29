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

import com.facebook.presto.rcfile.ColumnEncoding;
import com.facebook.presto.rcfile.RcFileEncoding;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.rcfile.RcFileDecoderUtils.checkType;

public class TextRcFileEncoding
        implements RcFileEncoding
{
    public static final byte[] DEFAULT_SEPARATORS = new byte[] {
            1,  // Start of Heading
            2,  // Start of text
            3,  // End of Text
            4,  // End of Transmission
            5,  // Enquiry
            6,  // Acknowledge
            7,  // Bell
            8,  // Backspace
            // RESERVED 9,  // Horizontal Tab
            // RESERVED 10, // Line Feed
            11, // Vertical Tab
            // RESERVED 12, // Form Feed
            // RESERVED 13, // Carriage Return
            14, // Shift Out
            15, // Shift In
            16, // Data Link Escape
            17, // Device Control One
            18, // Device Control Two
            19, // Device Control Three
            20, // Device Control Four
            21, // Negative Acknowledge
            22, // Synchronous Idle
            23, // End of Transmission Block
            24, // Cancel
            25, // End of medium
            26, // Substitute
            // RESERVED 27, // Escape
            28, // File Separator
            29, // Group separator
            // RESERVED 30, // Record Separator
            // RESERVED 31, // Unit separator
    };
    public static final Slice DEFAULT_NULL_SEQUENCE = Slices.utf8Slice("\\N");

    private final DateTimeZone hiveStorageTimeZone;
    private final Slice nullSequence;
    private final byte[] separators;
    private final Byte escapeByte;
    private final boolean lastColumnTakesRest;

    public TextRcFileEncoding(DateTimeZone hiveStorageTimeZone)
    {
        this(hiveStorageTimeZone,
                DEFAULT_NULL_SEQUENCE,
                DEFAULT_SEPARATORS,
                null,
                false);
    }

    public TextRcFileEncoding(DateTimeZone hiveStorageTimeZone, Slice nullSequence, byte[] separators, Byte escapeByte, boolean lastColumnTakesRest)
    {
        this.hiveStorageTimeZone = hiveStorageTimeZone;
        this.nullSequence = nullSequence;
        this.separators = separators;
        this.escapeByte = escapeByte;
        this.lastColumnTakesRest = lastColumnTakesRest;
    }

    @Override
    public ColumnEncoding booleanEncoding(Type type)
    {
        return new BooleanEncoding(type);
    }

    @Override
    public ColumnEncoding byteEncoding(Type type)
    {
        return longEncoding(type);
    }

    @Override
    public ColumnEncoding shortEncoding(Type type)
    {
        return longEncoding(type);
    }

    @Override
    public ColumnEncoding intEncoding(Type type)
    {
        return longEncoding(type);
    }

    @Override
    public ColumnEncoding longEncoding(Type type)
    {
        return new LongEncoding(type, nullSequence);
    }

    @Override
    public ColumnEncoding decimalEncoding(Type type)
    {
        return new DecimalEncoding(type, nullSequence);
    }

    @Override
    public ColumnEncoding floatEncoding(Type type)
    {
        return new FloatEncoding(type, nullSequence);
    }

    @Override
    public ColumnEncoding doubleEncoding(Type type)
    {
        return new DoubleEncoding(type, nullSequence);
    }

    @Override
    public ColumnEncoding stringEncoding(Type type)
    {
        return new StringEncoding(type, nullSequence, escapeByte);
    }

    @Override
    public ColumnEncoding binaryEncoding(Type type)
    {
        // binary text encoding is not escaped
        return new BinaryEncoding(type, nullSequence);
    }

    @Override
    public ColumnEncoding dateEncoding(Type type)
    {
        return new DateEncoding(type, nullSequence);
    }

    @Override
    public ColumnEncoding timestampEncoding(Type type)
    {
        return new TimestampEncoding(type, nullSequence, hiveStorageTimeZone);
    }

    @Override
    public ColumnEncoding listEncoding(Type type, ColumnEncoding elementEncoding)
    {
        return new ListEncoding(
                type,
                nullSequence,
                separators,
                escapeByte,
                checkType(elementEncoding, TextColumnEncoding.class, "elementEncoding"));
    }

    @Override
    public ColumnEncoding mapEncoding(Type type, ColumnEncoding keyEncoding, ColumnEncoding valueEncoding)
    {
        return new MapEncoding(
                type,
                nullSequence,
                separators,
                escapeByte,
                checkType(keyEncoding, TextColumnEncoding.class, "keyEncoding"),
                checkType(valueEncoding, TextColumnEncoding.class, "valueEncoding"));
    }

    @Override
    public ColumnEncoding structEncoding(Type type, List<ColumnEncoding> fieldEncodings)
    {
        return new StructEncoding(
                type,
                nullSequence,
                separators,
                escapeByte,
                lastColumnTakesRest,
                fieldEncodings.stream()
                        .map(field -> checkType(field, TextColumnEncoding.class, "fieldEncoding"))
                        .collect(Collectors.toList()));
    }
}
