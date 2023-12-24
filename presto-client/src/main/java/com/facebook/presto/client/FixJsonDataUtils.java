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
package com.facebook.presto.client;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.type.NamedTypeSignature;
import com.facebook.presto.common.type.ParameterKind;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.airlift.json.JsonCodec.mapJsonCodec;
import static com.facebook.presto.common.type.StandardTypes.ARRAY;
import static com.facebook.presto.common.type.StandardTypes.BIGINT;
import static com.facebook.presto.common.type.StandardTypes.BING_TILE;
import static com.facebook.presto.common.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.common.type.StandardTypes.CHAR;
import static com.facebook.presto.common.type.StandardTypes.DATE;
import static com.facebook.presto.common.type.StandardTypes.DECIMAL;
import static com.facebook.presto.common.type.StandardTypes.DOUBLE;
import static com.facebook.presto.common.type.StandardTypes.GEOMETRY;
import static com.facebook.presto.common.type.StandardTypes.INTEGER;
import static com.facebook.presto.common.type.StandardTypes.INTERVAL_DAY_TO_SECOND;
import static com.facebook.presto.common.type.StandardTypes.INTERVAL_YEAR_TO_MONTH;
import static com.facebook.presto.common.type.StandardTypes.IPADDRESS;
import static com.facebook.presto.common.type.StandardTypes.IPPREFIX;
import static com.facebook.presto.common.type.StandardTypes.JSON;
import static com.facebook.presto.common.type.StandardTypes.MAP;
import static com.facebook.presto.common.type.StandardTypes.REAL;
import static com.facebook.presto.common.type.StandardTypes.ROW;
import static com.facebook.presto.common.type.StandardTypes.SMALLINT;
import static com.facebook.presto.common.type.StandardTypes.TIME;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.StandardTypes.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.StandardTypes.TINYINT;
import static com.facebook.presto.common.type.StandardTypes.UUID;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

final class FixJsonDataUtils
{
    private static final JsonCodec<List<String>> LIST_JSON_CODEC = listJsonCodec(String.class);
    private static final JsonCodec<Map<String, String>> MAP_JSON_CODEC = mapJsonCodec(String.class, String.class);

    private FixJsonDataUtils() {}

    public static Iterable<List<Object>> fixData(List<Column> columns, Iterable<List<Object>> data)
    {
        if (data == null) {
            return null;
        }
        requireNonNull(columns, "columns is null");
        List<TypeSignature> signatures = columns.stream()
                .map(column -> parseTypeSignature(column.getType()))
                .collect(toList());
        ImmutableList.Builder<List<Object>> rows = ImmutableList.builder();
        for (List<Object> row : data) {
            checkArgument(row.size() == columns.size(), "row/column size mismatch");
            List<Object> newRow = new ArrayList<>();
            for (int i = 0; i < row.size(); i++) {
                newRow.add(fixValue(signatures.get(i), row.get(i)));
            }
            rows.add(unmodifiableList(newRow)); // allow nulls in list
        }
        return rows.build();
    }

    /**
     * Force values coming from Jackson to have the expected object type.
     */
    private static Object fixValue(TypeSignature signature, Object value)
    {
        if (value == null) {
            return null;
        }
        if (signature.isDistinctType()) {
            return fixValue(signature.getDistinctTypeInfo().getBaseType(), value);
        }
        if (signature.getTypeSignatureBase().hasTypeName() && signature.getTypeSignatureBase().hasStandardType()) {
            return fixValue(signature.getStandardTypeSignature(), value);
        }
        if (signature.getBase().equals(ARRAY)) {
            if (List.class.isAssignableFrom(value.getClass())) {
                List<Object> fixedValue = new ArrayList<>();
                for (Object object : List.class.cast(value)) {
                    fixedValue.add(fixValue(signature.getTypeParametersAsTypeSignatures().get(0), object));
                }
                return fixedValue;
            }
            // JSON keys may be serialized and deserialized as simple strings
            else if (value.getClass() == String.class) {
                List<String> newValue = LIST_JSON_CODEC.fromJson(value.toString());
                return fixValue(signature, newValue);
            }
            throw new IllegalArgumentException(String.format("Unexpected type found: %s", value.getClass()));
        }
        if (signature.getBase().equals(MAP)) {
            if (Map.class.isAssignableFrom(value.getClass())) {
                TypeSignature keySignature = signature.getTypeParametersAsTypeSignatures().get(0);
                TypeSignature valueSignature = signature.getTypeParametersAsTypeSignatures().get(1);
                Map<Object, Object> fixedValue = new HashMap<>();
                for (Map.Entry<?, ?> entry : (Set<Map.Entry<?, ?>>) Map.class.cast(value).entrySet()) {
                    fixedValue.put(fixValue(keySignature, entry.getKey()), fixValue(valueSignature, entry.getValue()));
                }
                return fixedValue;
            }
            // JSON keys may be serialized and deserialized as simple strings
            else if (value.getClass() == String.class) {
                Map<String, String> newValue = MAP_JSON_CODEC.fromJson(value.toString());
                return fixValue(signature, newValue);
            }
            throw new IllegalArgumentException(String.format("Unexpected type found: %s", value.getClass()));
        }
        if (signature.getBase().equals(ROW)) {
            if (List.class.isAssignableFrom(value.getClass())) {
                Map<String, Object> fixedValue = new LinkedHashMap<>();
                List<Object> listValue = List.class.cast(value);
                checkArgument(listValue.size() == signature.getParameters().size(), "Mismatched data values and row type");
                for (int i = 0; i < listValue.size(); i++) {
                    TypeSignatureParameter parameter = signature.getParameters().get(i);
                    checkArgument(
                            parameter.getKind() == ParameterKind.NAMED_TYPE,
                            "Unexpected parameter [%s] for row type",
                            parameter);
                    NamedTypeSignature namedTypeSignature = parameter.getNamedTypeSignature();
                    String key = namedTypeSignature.getName().orElse("field" + i);
                    fixedValue.put(key, fixValue(namedTypeSignature.getTypeSignature(), listValue.get(i)));
                }
                return fixedValue;
            }
            else if (value.getClass() == String.class) {
                List<String> newValue = LIST_JSON_CODEC.fromJson(value.toString());
                return fixValue(signature, newValue);
            }
            throw new IllegalArgumentException(String.format("Unexpected type found: %s", value.getClass()));
        }
        if (signature.isVarcharEnum()) {
            return String.class.cast(value);
        }
        if (signature.isBigintEnum()) {
            if (value instanceof String) {
                return Long.parseLong((String) value);
            }
            return ((Number) value).longValue();
        }
        switch (signature.getBase()) {
            case BIGINT:
                if (value instanceof String) {
                    return Long.parseLong((String) value);
                }
                return ((Number) value).longValue();
            case INTEGER:
                if (value instanceof String) {
                    return Integer.parseInt((String) value);
                }
                return ((Number) value).intValue();
            case SMALLINT:
                if (value instanceof String) {
                    return Short.parseShort((String) value);
                }
                return ((Number) value).shortValue();
            case TINYINT:
                if (value instanceof String) {
                    return Byte.parseByte((String) value);
                }
                return ((Number) value).byteValue();
            case DOUBLE:
                if (value instanceof String) {
                    return Double.parseDouble((String) value);
                }
                return ((Number) value).doubleValue();
            case REAL:
                if (value instanceof String) {
                    return Float.parseFloat((String) value);
                }
                return ((Number) value).floatValue();
            case BOOLEAN:
                if (value instanceof String) {
                    return Boolean.parseBoolean((String) value);
                }
                return Boolean.class.cast(value);
            case VARCHAR:
            case JSON:
            case TIME:
            case TIME_WITH_TIME_ZONE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIME_ZONE:
            case DATE:
            case INTERVAL_YEAR_TO_MONTH:
            case INTERVAL_DAY_TO_SECOND:
            case IPADDRESS:
            case IPPREFIX:
            case DECIMAL:
            case CHAR:
            case GEOMETRY:
            case UUID:
                return String.class.cast(value);
            case BING_TILE:
                // Bing tiles are serialized as strings when used as map keys,
                // they are serialized as json otherwise (value will be a LinkedHashMap).
                return value;
            default:
                // for now we assume that only the explicit types above are passed
                // as a plain text and everything else is base64 encoded binary
                if (value instanceof String) {
                    return Base64.getDecoder().decode((String) value);
                }
                return value;
        }
    }
}
