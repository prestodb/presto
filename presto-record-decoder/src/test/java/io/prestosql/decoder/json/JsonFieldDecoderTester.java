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
package io.prestosql.decoder.json;

import com.google.common.collect.ImmutableSet;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.DecoderTestColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.decoder.RowDecoder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.Type;

import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.util.Preconditions.checkArgument;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class JsonFieldDecoderTester
{
    private static final JsonRowDecoderFactory DECODER_FACTORY = new JsonRowDecoderFactory(new ObjectMapperProvider().get());

    private Optional<String> dataFormat;
    private Optional<String> formatHint;

    public JsonFieldDecoderTester()
    {
        this(Optional.empty(), Optional.empty());
    }

    public JsonFieldDecoderTester(String dataFormat)
    {
        this(Optional.of(dataFormat), Optional.empty());
    }

    public JsonFieldDecoderTester(String dataFormat, String formatHint)
    {
        this(Optional.of(dataFormat), Optional.of(formatHint));
    }

    private JsonFieldDecoderTester(Optional<String> dataFormat, Optional<String> formatHint)
    {
        this.dataFormat = requireNonNull(dataFormat, "dataFormat is null");
        this.formatHint = requireNonNull(formatHint, "formatHint is null");
    }

    public void assertDecodedAs(String jsonValue, Type type, long expectedValue)
    {
        checkArgument(type.getJavaType() == long.class, "Wrong (not long based) presto type '%s'", type);
        FieldValueProvider decodedValue = decode(Optional.of(jsonValue), type);
        assertFalse(decodedValue.isNull(), format("expected non null when decoding %s as %s", jsonValue, type));
        assertEquals(decodedValue.getLong(), expectedValue);
    }

    public void assertDecodedAs(String jsonValue, Type type, double expectedValue)
    {
        checkArgument(type.getJavaType() == double.class, "Wrong (not double based) presto type '%s'", type);
        FieldValueProvider decodedValue = decode(Optional.of(jsonValue), type);
        assertFalse(decodedValue.isNull(), format("expected non null when decoding %s as %s", jsonValue, type));
        assertEquals(decodedValue.getDouble(), expectedValue);
    }

    public void assertDecodedAs(String jsonValue, Type type, Slice expectedValue)
    {
        checkArgument(type.getJavaType() == Slice.class, "Wrong (not Slice based) presto type '%s'", type);
        FieldValueProvider decodedValue = decode(Optional.of(jsonValue), type);
        assertFalse(decodedValue.isNull(), format("expected non null when decoding %s as %s", jsonValue, type));
        assertEquals(decodedValue.getSlice(), expectedValue);
    }

    public void assertDecodedAs(String jsonValue, Type type, boolean expectedValue)
    {
        checkArgument(type.getJavaType() == boolean.class, "Wrong (not boolean based) presto type '%s'", type);
        FieldValueProvider decodedValue = decode(Optional.of(jsonValue), type);
        assertFalse(decodedValue.isNull(), format("expected non null when decoding %s as %s", jsonValue, type));
        assertEquals(decodedValue.getBoolean(), expectedValue);
    }

    public void assertDecodedAsNull(String jsonValue, Type type)
    {
        FieldValueProvider decodedValue = decode(Optional.of(jsonValue), type);
        assertTrue(decodedValue.isNull(), format("expected null when decoding %s as %s", jsonValue, type));
    }

    public void assertMissingDecodedAsNull(Type type)
    {
        FieldValueProvider decodedValue = decode(Optional.empty(), type);
        assertTrue(decodedValue.isNull(), format("expected null when decoding missing field as %s", type));
    }

    public void assertInvalidInput(String jsonValue, Type type, String exceptionRegex)
    {
        assertThatThrownBy(() -> decode(Optional.of(jsonValue), type).getLong())
                .isInstanceOf(PrestoException.class)
                .hasMessageMatching(exceptionRegex);
    }

    private FieldValueProvider decode(Optional<String> jsonValue, Type type)
    {
        String jsonField = "value";
        String json = jsonValue.map(value -> format("{\"%s\":%s}", jsonField, value)).orElse("{}");
        DecoderTestColumnHandle columnHandle = new DecoderTestColumnHandle(
                0,
                "some_column",
                type,
                jsonField,
                dataFormat.orElse(null),
                formatHint.orElse(null),
                false,
                false,
                false);

        RowDecoder rowDecoder = DECODER_FACTORY.create(emptyMap(), ImmutableSet.of(columnHandle));
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = rowDecoder.decodeRow(json.getBytes(UTF_8), null)
                .orElseThrow(AssertionError::new);
        assertTrue(decodedRow.containsKey(columnHandle), format("column '%s' not found in decoded row", columnHandle.getName()));
        return decodedRow.get(columnHandle);
    }
}
