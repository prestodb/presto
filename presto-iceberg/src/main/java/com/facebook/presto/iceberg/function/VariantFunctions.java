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
package com.facebook.presto.iceberg.function;

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.iceberg.VariantBinaryCodec;
import com.facebook.presto.iceberg.VariantBinaryCodec.VariantBinary;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

/**
 * SQL scalar functions for working with Iceberg V3 Variant data.
 *
 * <p>Variant data in Presto is stored as VARCHAR (JSON strings). These functions
 * provide field extraction (with dot-path and array indexing), validation,
 * normalization, type introspection, key enumeration, binary round-trip,
 * and explicit cast capabilities for Variant values.
 *
 * <p>Functions are registered via {@code IcebergConnector.getSystemFunctions()}
 * and accessed as {@code iceberg.system.<function_name>(...)}.
 *
 * <h3>Phase 2: Binary Interoperability</h3>
 * <p>{@code parse_variant} and {@code variant_binary_roundtrip} exercise the
 * {@link VariantBinaryCodec} which implements the Apache Variant binary spec (v1).
 * Full Parquet read/write path integration (transparent binary decode/encode in
 * {@code IcebergPageSourceProvider} / {@code IcebergPageSink}) is documented as
 * a future enhancement — the codec is ready; the page source wiring requires
 * detecting VARIANT columns at the Parquet schema level.
 *
 * <h3>Phase 4: Predicate Pushdown</h3>
 * <p>{@code IS NULL} / {@code IS NOT NULL} predicates on VARIANT columns already
 * work through the VARCHAR type mapping. Pushdown of {@code variant_get(col, 'field') = 'value'}
 * would require an optimizer rule to rewrite the expression into a domain constraint,
 * which is tracked as future work.
 */
public final class VariantFunctions
{
    private static final JsonFactory JSON_FACTORY = new JsonFactory();

    private VariantFunctions() {}

    // ---- Phase 3: Enhanced variant_get with dot-path and array indexing ----

    /**
     * Extracts a value from a Variant (JSON) by path expression.
     * Supports dot-notation for nested objects and bracket notation for arrays.
     *
     * <p>Path syntax:
     * <ul>
     *   <li>{@code 'name'} — top-level field</li>
     *   <li>{@code 'address.city'} — nested field via dot-notation</li>
     *   <li>{@code 'items[0]'} — array element by index</li>
     *   <li>{@code 'users[0].name'} — combined path</li>
     * </ul>
     *
     * <p>Returns NULL if the path doesn't exist, the input is invalid JSON,
     * or a path segment references a non-existent field/index.
     * For complex values (objects/arrays), returns the JSON string representation.
     *
     * <p>Usage: {@code variant_get('{"users":[{"name":"Alice"}]}', 'users[0].name')} → {@code 'Alice'}
     */
    @ScalarFunction("variant_get")
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice variantGet(
            @SqlType(StandardTypes.VARCHAR) Slice variant,
            @SqlType(StandardTypes.VARCHAR) Slice path)
    {
        if (variant == null || path == null) {
            return null;
        }

        String json = variant.toStringUtf8();
        String pathStr = path.toStringUtf8();
        List<PathSegment> segments = parsePath(pathStr);

        try {
            String current = json;
            for (PathSegment segment : segments) {
                if (current == null) {
                    return null;
                }
                if (segment.isArrayIndex) {
                    current = extractArrayElement(current, segment.arrayIndex);
                }
                else {
                    current = extractObjectField(current, segment.fieldName);
                }
            }
            return current != null ? Slices.utf8Slice(current) : null;
        }
        catch (IOException e) {
            return null;
        }
    }

    // ---- Phase 3: variant_keys ----

    /**
     * Returns the top-level keys of a Variant JSON object as a JSON array.
     * Returns NULL if the input is not a JSON object.
     *
     * <p>Usage: {@code variant_keys('{"name":"Alice","age":30}')} → {@code '["name","age"]'}
     */
    @ScalarFunction("variant_keys")
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice variantKeys(@SqlType(StandardTypes.VARCHAR) Slice variant)
    {
        if (variant == null) {
            return null;
        }

        String json = variant.toStringUtf8();
        try (JsonParser parser = JSON_FACTORY.createParser(json)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                return null;
            }

            StringWriter writer = new StringWriter();
            try (JsonGenerator gen = JSON_FACTORY.createGenerator(writer)) {
                gen.writeStartArray();
                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    gen.writeString(parser.getCurrentName());
                    parser.nextToken();
                    parser.skipChildren();
                }
                gen.writeEndArray();
            }
            return Slices.utf8Slice(writer.toString());
        }
        catch (IOException e) {
            return null;
        }
    }

    // ---- Phase 3: variant_type ----

    /**
     * Returns the JSON type of a Variant value as a string.
     * Possible return values: "object", "array", "string", "number", "boolean", "null".
     * Returns NULL if the input cannot be parsed.
     *
     * <p>Usage: {@code variant_type('{"a":1}')} → {@code 'object'}
     */
    @ScalarFunction("variant_type")
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice variantType(@SqlType(StandardTypes.VARCHAR) Slice variant)
    {
        if (variant == null) {
            return null;
        }

        String json = variant.toStringUtf8();
        try (JsonParser parser = JSON_FACTORY.createParser(json)) {
            JsonToken token = parser.nextToken();
            if (token == null) {
                return null;
            }
            switch (token) {
                case START_OBJECT: return Slices.utf8Slice("object");
                case START_ARRAY: return Slices.utf8Slice("array");
                case VALUE_STRING: return Slices.utf8Slice("string");
                case VALUE_NUMBER_INT:
                case VALUE_NUMBER_FLOAT:
                    return Slices.utf8Slice("number");
                case VALUE_TRUE:
                case VALUE_FALSE:
                    return Slices.utf8Slice("boolean");
                case VALUE_NULL: return Slices.utf8Slice("null");
                default: return null;
            }
        }
        catch (IOException e) {
            return null;
        }
    }

    // ---- Phase 5: to_variant (explicit cast) ----

    /**
     * Validates a JSON string and returns it as a Variant value.
     * This is the explicit cast function from VARCHAR to VARIANT.
     * Throws an error if the input is not valid JSON.
     *
     * <p>Since VARIANT is represented as VARCHAR in Presto, this function serves
     * as the explicit validation boundary — it guarantees the output is well-formed JSON.
     *
     * <p>Usage: {@code to_variant('{"name":"Alice"}')} → {@code '{"name":"Alice"}'}
     */
    @ScalarFunction("to_variant")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice toVariant(@SqlType(StandardTypes.VARCHAR) Slice json)
    {
        String input = json.toStringUtf8();
        try {
            StringWriter writer = new StringWriter();
            try (JsonParser parser = JSON_FACTORY.createParser(input);
                    JsonGenerator gen = JSON_FACTORY.createGenerator(writer)) {
                JsonToken token = parser.nextToken();
                if (token == null) {
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Empty input is not valid Variant JSON");
                }
                gen.copyCurrentStructure(parser);
                if (parser.nextToken() != null) {
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT,
                            "Trailing content after JSON value");
                }
            }
            return Slices.utf8Slice(writer.toString());
        }
        catch (PrestoException e) {
            throw e;
        }
        catch (IOException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT,
                    "Invalid JSON for Variant: " + e.getMessage(), e);
        }
    }

    // ---- Phase 2: parse_variant (binary codec validation) ----

    /**
     * Parses and validates a JSON string as a Variant value by encoding it
     * to Variant binary format (Apache Iceberg V3 spec) and decoding back.
     * Returns the normalized (compact) JSON representation.
     * Throws if the input is not valid JSON.
     *
     * <p>This exercises the full binary codec round-trip, validating that
     * the data can be represented in Variant binary format for interoperability
     * with other engines (Spark, Trino).
     *
     * <p>Usage: {@code parse_variant('{"name":"Alice"}')} → {@code '{"name":"Alice"}'}
     */
    @ScalarFunction("parse_variant")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice parseVariant(@SqlType(StandardTypes.VARCHAR) Slice json)
    {
        String input = json.toStringUtf8();
        try {
            VariantBinary binary = VariantBinaryCodec.fromJson(input);
            String normalized = VariantBinaryCodec.toJson(binary.getMetadata(), binary.getValue());
            return Slices.utf8Slice(normalized);
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT,
                    "Invalid JSON for Variant: " + e.getMessage(), e);
        }
    }

    // ---- Phase 2: variant_to_json ----

    /**
     * Converts a Variant value to its normalized JSON string representation.
     * Normalizes the JSON through Jackson round-trip (compact form).
     *
     * <p>Usage: {@code variant_to_json(variant_column)} → {@code '{"name":"Alice"}'}
     */
    @ScalarFunction("variant_to_json")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice variantToJson(@SqlType(StandardTypes.VARCHAR) Slice variant)
    {
        String input = variant.toStringUtf8();
        try {
            StringWriter writer = new StringWriter();
            try (JsonParser parser = JSON_FACTORY.createParser(input);
                    JsonGenerator gen = JSON_FACTORY.createGenerator(writer)) {
                parser.nextToken();
                gen.copyCurrentStructure(parser);
            }
            return Slices.utf8Slice(writer.toString());
        }
        catch (IOException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT,
                    "Invalid Variant JSON: " + e.getMessage(), e);
        }
    }

    // ---- Phase 2: variant_binary_roundtrip ----

    /**
     * Encodes a JSON string into Variant binary format (Apache Iceberg V3 spec)
     * and decodes it back to JSON. Validates binary round-trip fidelity.
     * Useful for testing binary interoperability with other engines (Spark, Trino).
     *
     * <p>Usage: {@code variant_binary_roundtrip('{"a":1}')} → {@code '{"a":1}'}
     */
    @ScalarFunction("variant_binary_roundtrip")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice variantBinaryRoundtrip(@SqlType(StandardTypes.VARCHAR) Slice json)
    {
        String input = json.toStringUtf8();
        try {
            VariantBinary binary = VariantBinaryCodec.fromJson(input);
            String decoded = VariantBinaryCodec.toJson(binary.getMetadata(), binary.getValue());
            return Slices.utf8Slice(decoded);
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT,
                    "Failed Variant binary round-trip: " + e.getMessage(), e);
        }
    }

    // ---- Path parsing and JSON navigation helpers ----

    private static final class PathSegment
    {
        final String fieldName;
        final int arrayIndex;
        final boolean isArrayIndex;

        PathSegment(String fieldName)
        {
            this.fieldName = fieldName;
            this.arrayIndex = -1;
            this.isArrayIndex = false;
        }

        PathSegment(int arrayIndex)
        {
            this.fieldName = null;
            this.arrayIndex = arrayIndex;
            this.isArrayIndex = true;
        }
    }

    static List<PathSegment> parsePath(String path)
    {
        List<PathSegment> segments = new ArrayList<>();
        StringBuilder current = new StringBuilder();

        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (c == '.') {
                if (current.length() > 0) {
                    segments.add(new PathSegment(current.toString()));
                    current.setLength(0);
                }
            }
            else if (c == '[') {
                if (current.length() > 0) {
                    segments.add(new PathSegment(current.toString()));
                    current.setLength(0);
                }
                int end = path.indexOf(']', i);
                if (end == -1) {
                    segments.add(new PathSegment(path.substring(i)));
                    return segments;
                }
                String indexStr = path.substring(i + 1, end);
                try {
                    segments.add(new PathSegment(Integer.parseInt(indexStr)));
                }
                catch (NumberFormatException e) {
                    segments.add(new PathSegment(indexStr));
                }
                i = end;
            }
            else {
                current.append(c);
            }
        }

        if (current.length() > 0) {
            segments.add(new PathSegment(current.toString()));
        }
        return segments;
    }

    private static String extractObjectField(String json, String fieldName) throws IOException
    {
        try (JsonParser parser = JSON_FACTORY.createParser(json)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                return null;
            }

            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String currentField = parser.getCurrentName();
                JsonToken valueToken = parser.nextToken();

                if (fieldName.equals(currentField)) {
                    if (valueToken == JsonToken.VALUE_NULL) {
                        return "null";
                    }
                    if (valueToken == JsonToken.START_OBJECT || valueToken == JsonToken.START_ARRAY) {
                        StringWriter writer = new StringWriter();
                        try (JsonGenerator gen = JSON_FACTORY.createGenerator(writer)) {
                            gen.copyCurrentStructure(parser);
                        }
                        return writer.toString();
                    }
                    return parser.getText();
                }
                parser.skipChildren();
            }
        }
        return null;
    }

    private static String extractArrayElement(String json, int index) throws IOException
    {
        try (JsonParser parser = JSON_FACTORY.createParser(json)) {
            if (parser.nextToken() != JsonToken.START_ARRAY) {
                return null;
            }

            int currentIndex = 0;
            while (parser.nextToken() != JsonToken.END_ARRAY) {
                if (currentIndex == index) {
                    JsonToken token = parser.currentToken();
                    if (token == JsonToken.VALUE_NULL) {
                        return "null";
                    }
                    if (token == JsonToken.START_OBJECT || token == JsonToken.START_ARRAY) {
                        StringWriter writer = new StringWriter();
                        try (JsonGenerator gen = JSON_FACTORY.createGenerator(writer)) {
                            gen.copyCurrentStructure(parser);
                        }
                        return writer.toString();
                    }
                    return parser.getText();
                }
                parser.skipChildren();
                currentIndex++;
            }
        }
        return null;
    }
}
