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
package com.facebook.presto.plugin.base;

import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

public class JsonTypeUtil
{
    private static final JsonFactory JSON_FACTORY = new JsonFactoryBuilder().disable(CANONICALIZE_FIELD_NAMES).build();
    private static final ObjectMapper SORTED_MAPPER = new JsonObjectMapperProvider().get().configure(ORDER_MAP_ENTRIES_BY_KEYS, true);

    private JsonTypeUtil() {}

    // TODO: this should be available from the engine
    public static Slice jsonParse(Slice slice)
    {
        // cast(json_parse(x) AS t)` will be optimized into `$internal$json_string_to_array/map/row_cast` in ExpressionOptimizer
        // If you make changes to this function (e.g. use parse JSON string into some internal representation),
        // make sure `$internal$json_string_to_array/map/row_cast` is changed accordingly.
        try (JsonParser parser = createJsonParser(JSON_FACTORY, slice)) {
            SliceOutput dynamicSliceOutput = new DynamicSliceOutput(slice.length());
            SORTED_MAPPER.writeValue((OutputStream) dynamicSliceOutput, SORTED_MAPPER.readValue(parser, Object.class));
            // nextToken() returns null if the input is parsed correctly,
            // but will throw an exception if there are trailing characters.
            parser.nextToken();
            return dynamicSliceOutput.slice();
        }
        catch (IOException | RuntimeException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Cannot convert value to JSON: '%s'", slice.toStringUtf8()), e);
        }
    }

    public static Slice toJsonValue(Object value)
            throws IOException
    {
        return Slices.wrappedBuffer(SORTED_MAPPER.writeValueAsBytes(value));
    }

    private static JsonParser createJsonParser(JsonFactory factory, Slice json)
            throws IOException
    {
        // Jackson tries to detect the character encoding automatically when
        // using InputStream, so we pass an InputStreamReader instead.
        return factory.createParser(new InputStreamReader(json.getInput(), UTF_8));
    }
}
