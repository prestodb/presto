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
package com.facebook.presto.plugin.opensearch;

import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;

/**
 * Extracts nested field values from OpenSearch documents.
 * Handles navigation through nested object structures and type conversion.
 *
 * Example:
 * Document: {"token_usage": {"total_tokens": 90616}}
 * Path: ["token_usage", "total_tokens"]
 * Result: 90616 (as Long)
 *
 * Null Handling:
 * - Returns null if any part of the path doesn't exist
 * - Returns null if the value is explicitly null
 * - Returns null if type conversion fails
 */
public class NestedValueExtractor
{
    private static final Logger log = Logger.get(NestedValueExtractor.class);
    private static final ObjectMapper OBJECT_MAPPER = new JsonObjectMapperProvider().get();

    /**
     * Extracts a nested field value from a document.
     *
     * @param document The OpenSearch document
     * @param fieldPath The path to the field (e.g., ["token_usage", "total_tokens"])
     * @param targetType The expected Presto type
     * @return The extracted value, or null if not found or conversion fails
     */
    public Object extractNestedValue(
            Map<String, Object> document,
            List<String> fieldPath,
            Type targetType)
    {
        if (fieldPath == null || fieldPath.isEmpty()) {
            return null;
        }

        if (document == null) {
            return null;
        }

        Object current = document;

        // Navigate through the path
        for (int i = 0; i < fieldPath.size(); i++) {
            String pathSegment = fieldPath.get(i);

            if (current == null) {
                // Parent is null, cannot navigate further
                return null;
            }

            if (current instanceof Map) {
                Map<String, Object> map = (Map<String, Object>) current;
                if (!map.containsKey(pathSegment)) {
                    // Field doesn't exist in this document
                    return null;
                }
                current = map.get(pathSegment);
            }
            else {
                // Path doesn't exist (parent is not an object)
                return null;
            }
        }

        // current is now the field value (or null)
        return convertToPrestoType(current, targetType);
    }

    /**
     * Converts an OpenSearch value to the appropriate Presto type.
     * Returns null if conversion fails.
     *
     * @param value The value to convert
     * @param targetType The target Presto type
     * @return The converted value, or null if conversion fails
     */
    private Object convertToPrestoType(Object value, Type targetType)
    {
        if (value == null) {
            return null;
        }

        try {
            if (targetType.equals(BIGINT)) {
                if (value instanceof Number) {
                    return ((Number) value).longValue();
                }
                return Long.parseLong(value.toString());
            }
            else if (targetType.equals(INTEGER)) {
                if (value instanceof Number) {
                    return ((Number) value).intValue();
                }
                return Integer.parseInt(value.toString());
            }
            else if (targetType.equals(SMALLINT)) {
                if (value instanceof Number) {
                    return ((Number) value).shortValue();
                }
                return Short.parseShort(value.toString());
            }
            else if (targetType.equals(TINYINT)) {
                if (value instanceof Number) {
                    return ((Number) value).byteValue();
                }
                return Byte.parseByte(value.toString());
            }
            else if (targetType.equals(DOUBLE)) {
                if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                }
                return Double.parseDouble(value.toString());
            }
            else if (targetType.equals(REAL)) {
                if (value instanceof Number) {
                    return ((Number) value).floatValue();
                }
                return Float.parseFloat(value.toString());
            }
            else if (targetType.equals(BOOLEAN)) {
                if (value instanceof Boolean) {
                    return value;
                }
                return Boolean.parseBoolean(value.toString());
            }
            else if (targetType.equals(TIMESTAMP)) {
                // Handle timestamp conversion
                if (value instanceof Number) {
                    return ((Number) value).longValue();
                }
                return value.toString();
            }
            else if (targetType.equals(VARBINARY)) {
                // Handle binary data
                if (value instanceof byte[]) {
                    return value;
                }
                return value.toString().getBytes();
            }
            else if (targetType instanceof ArrayType) {
                // Handle array types - return the value as-is if it's already a List or array
                // The OpenSearchPageSource will handle the conversion to Presto blocks
                if (value instanceof List) {
                    return value;
                }
                else if (value instanceof Object[]) {
                    // Convert Object[] to List for consistent handling
                    Object[] array = (Object[]) value;
                    List<Object> list = new ArrayList<>(array.length);
                    for (Object element : array) {
                        list.add(element);
                    }
                    return list;
                }
                else {
                    // Single value - wrap in a list
                    List<Object> list = new ArrayList<>(1);
                    list.add(value);
                    return list;
                }
            }
            else if (targetType.equals(VARCHAR)) {
                // For VARCHAR, if the value is a complex object (Map or List), convert to JSON
                if (value instanceof Map || value instanceof List) {
                    try {
                        return OBJECT_MAPPER.writeValueAsString(value);
                    }
                    catch (Exception e) {
                        log.warn(e, "Failed to convert object to JSON, using toString()");
                        return value.toString();
                    }
                }
                return value.toString();
            }

            // Default: return as string
            log.debug("Unknown type conversion for %s, returning as string", targetType);
            return value.toString();
        }
        catch (Exception e) {
            // If conversion fails, return null and log warning
            log.warn(e, "Type conversion failed for value '%s' to type %s", value, targetType);
            return null;
        }
    }

    /**
     * Extracts a flat (non-nested) field value from a document.
     * This is a convenience method for extracting top-level fields.
     *
     * @param document The OpenSearch document
     * @param fieldName The field name
     * @param targetType The expected Presto type
     * @return The extracted value, or null if not found
     */
    public Object extractFlatValue(
            Map<String, Object> document,
            String fieldName,
            Type targetType)
    {
        if (document == null || fieldName == null) {
            return null;
        }

        Object value = document.get(fieldName);
        return convertToPrestoType(value, targetType);
    }
}
