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
package com.facebook.presto.plugin.opensearch.types;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;

import java.util.Locale;
import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;

/**
 * Maps OpenSearch field types to Presto types.
 * Provides robust error handling for unknown or malformed type names.
 */
public final class TypeMapper
{
    private static final Logger log = Logger.get(TypeMapper.class);

    private TypeMapper() {}

    public static Type toPrestoType(String openSearchType, Map<String, Object> properties, String fieldName)
    {
        // Validate input - protect against null or malformed type strings
        if (openSearchType == null || openSearchType.trim().isEmpty()) {
            log.warn("Null or empty OpenSearch type for field '%s', defaulting to VARCHAR", fieldName);
            return VARCHAR;
        }

        // Sanitize type string - remove any field name prefix if present
        String sanitizedType = sanitizeTypeString(openSearchType, fieldName);

        // Check if this is an embedding/vector field that should be treated as an array
        if (fieldName != null && (fieldName.equals("embedding") || fieldName.contains("vector") || fieldName.contains("embedding"))) {
            if ("float".equalsIgnoreCase(sanitizedType) || "half_float".equalsIgnoreCase(sanitizedType)) {
                return new ArrayType(REAL);
            }
        }
        return toPrestoType(sanitizedType, properties);
    }

    public static Type toPrestoType(String openSearchType, Map<String, Object> properties)
    {
        // Validate input
        if (openSearchType == null || openSearchType.trim().isEmpty()) {
            log.warn("Null or empty OpenSearch type, defaulting to VARCHAR");
            return VARCHAR;
        }

        // Sanitize and normalize the type string
        String sanitizedType = sanitizeTypeString(openSearchType, null);

        switch (sanitizedType.toLowerCase(Locale.ENGLISH)) {
            case "text":
            case "keyword":
            case "ip":
            case "varchar":
                return VARCHAR;

            case "long":
            case "bigint":
                return BIGINT;

            case "integer":
                return INTEGER;

            case "short":
            case "smallint":
                return SMALLINT;

            case "byte":
            case "tinyint":
                return TINYINT;

            case "double":
            case "scaled_float":
                return DOUBLE;

            case "float":
            case "half_float":
            case "real":
                // Check if this is actually an array field (OpenSearch doesn't have explicit array types)
                // If the field name suggests it's an embedding/vector, treat it as an array
                // This is a heuristic for dynamically mapped array fields
                return REAL;

            case "boolean":
                return BOOLEAN;

            case "date":
                // OpenSearch date type - always map to Presto DATE
                // OpenSearchPageSource will handle the conversion from whatever format OpenSearch returns
                return DATE;

            case "binary":
            case "varbinary":
                return VARBINARY;

            case "knn_vector":
                // Vector fields are represented as ARRAY(REAL)
                return new ArrayType(REAL);

            case "nested":
            case "object":
                // For now, treat nested/object as VARCHAR (JSON string)
                // TODO: Implement proper ROW type mapping
                return VARCHAR;

            default:
                // Unknown types default to VARCHAR with warning
                log.warn("Unknown OpenSearch type '%s', defaulting to VARCHAR. " +
                        "This may indicate a custom type, plugin-provided type, or malformed type string. " +
                        "Consider adding explicit mapping if this type is commonly used.", sanitizedType);
                return VARCHAR;
        }
    }

    public static Type toPrestoType(String openSearchType)
    {
        return toPrestoType(openSearchType, Map.of());
    }

    /**
     * Sanitizes a type string to extract just the type name, removing any field name prefix.
     * Handles cases where type strings may be malformed like "field_name type_name".
     *
     * @param typeString The type string to sanitize
     * @param fieldName The field name (optional, used for better error messages)
     * @return Sanitized type string containing only the type name
     */
    private static String sanitizeTypeString(String typeString, String fieldName)
    {
        if (typeString == null) {
            return "";
        }

        String trimmed = typeString.trim();

        // Check if the type string contains spaces (potential malformed string like "field_name bigint")
        if (trimmed.contains(" ")) {
            String[] parts = trimmed.split("\\s+");

            // Try to find a known type in the parts
            // Check last part first (most common case: "field_name type")
            if (parts.length >= 2) {
                String lastPart = parts[parts.length - 1].toLowerCase(Locale.ENGLISH);
                if (isKnownType(lastPart)) {
                    log.warn("Type string '%s' for field '%s' appears malformed (contains field name). " +
                            "Extracted type '%s'. This may indicate a serialization issue.",
                            typeString, fieldName, lastPart);
                    return lastPart;
                }
            }

            // Check first part (case: "type field_name")
            String firstPart = parts[0].toLowerCase(Locale.ENGLISH);
            if (isKnownType(firstPart)) {
                log.warn("Type string '%s' for field '%s' appears malformed (contains field name). " +
                        "Extracted type '%s'. This may indicate a serialization issue.",
                        typeString, fieldName, firstPart);
                return firstPart;
            }

            // If we can't determine the correct part, log warning and use first part
            log.warn("Type string '%s' for field '%s' contains spaces and cannot be parsed reliably. " +
                    "Using first token '%s'. This may cause type mapping issues.",
                    typeString, fieldName, parts[0]);
            return parts[0];
        }

        return trimmed;
    }

    /**
     * Checks if a type string is a known OpenSearch/Presto type.
     *
     * @param type The type string to check
     * @return true if the type is known, false otherwise
     */
    private static boolean isKnownType(String type)
    {
        switch (type.toLowerCase(Locale.ENGLISH)) {
            case "text":
            case "keyword":
            case "ip":
            case "long":
            case "integer":
            case "short":
            case "byte":
            case "double":
            case "scaled_float":
            case "float":
            case "half_float":
            case "boolean":
            case "date":
            case "binary":
            case "knn_vector":
            case "nested":
            case "object":
            case "bigint":
            case "varchar":
            case "real":
            case "smallint":
            case "tinyint":
            case "varbinary":
                return true;
            default:
                return false;
        }
    }
}
