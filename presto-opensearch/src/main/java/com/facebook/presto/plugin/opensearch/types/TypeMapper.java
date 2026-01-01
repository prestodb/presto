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
 */
public final class TypeMapper
{
    private TypeMapper() {}

    public static Type toPrestoType(String openSearchType, Map<String, Object> properties, String fieldName)
    {
        // Check if this is an embedding/vector field that should be treated as an array
        if (fieldName != null && (fieldName.equals("embedding") || fieldName.contains("vector") || fieldName.contains("embedding"))) {
            if ("float".equalsIgnoreCase(openSearchType) || "half_float".equalsIgnoreCase(openSearchType)) {
                return new ArrayType(REAL);
            }
        }
        return toPrestoType(openSearchType, properties);
    }

    public static Type toPrestoType(String openSearchType, Map<String, Object> properties)
    {
        switch (openSearchType.toLowerCase(Locale.ENGLISH)) {
            case "text":
            case "keyword":
            case "ip":
                return VARCHAR;

            case "long":
                return BIGINT;

            case "integer":
                return INTEGER;

            case "short":
                return SMALLINT;

            case "byte":
                return TINYINT;

            case "double":
            case "scaled_float":
                return DOUBLE;

            case "float":
            case "half_float":
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
                // Unknown types default to VARCHAR
                return VARCHAR;
        }
    }

    public static Type toPrestoType(String openSearchType)
    {
        return toPrestoType(openSearchType, Map.of());
    }
}
