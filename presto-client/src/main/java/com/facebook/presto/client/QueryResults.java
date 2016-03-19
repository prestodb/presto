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

import com.facebook.presto.spi.type.NamedTypeSignature;
import com.facebook.presto.spi.type.ParameterKind;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.validation.constraints.NotNull;

import java.net.URI;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.StandardTypes.BIGINT;
import static com.facebook.presto.spi.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.spi.type.StandardTypes.DATE;
import static com.facebook.presto.spi.type.StandardTypes.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.INTERVAL_DAY_TO_SECOND;
import static com.facebook.presto.spi.type.StandardTypes.INTERVAL_YEAR_TO_MONTH;
import static com.facebook.presto.spi.type.StandardTypes.JSON;
import static com.facebook.presto.spi.type.StandardTypes.MAP;
import static com.facebook.presto.spi.type.StandardTypes.ROW;
import static com.facebook.presto.spi.type.StandardTypes.TIME;
import static com.facebook.presto.spi.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.spi.type.StandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.StandardTypes.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.unmodifiableIterable;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@Immutable
public class QueryResults
{
    private final String id;
    private final URI infoUri;
    private final URI partialCancelUri;
    private final URI nextUri;
    private final List<Column> columns;
    private final Iterable<List<Object>> data;
    private final StatementStats stats;
    private final QueryError error;
    private final String updateType;
    private final Long updateCount;

    @JsonCreator
    public QueryResults(
            @JsonProperty("id") String id,
            @JsonProperty("infoUri") URI infoUri,
            @JsonProperty("partialCancelUri") URI partialCancelUri,
            @JsonProperty("nextUri") URI nextUri,
            @JsonProperty("columns") List<Column> columns,
            @JsonProperty("data") List<List<Object>> data,
            @JsonProperty("stats") StatementStats stats,
            @JsonProperty("error") QueryError error,
            @JsonProperty("updateType") String updateType,
            @JsonProperty("updateCount") Long updateCount)
    {
        this(id, infoUri, partialCancelUri, nextUri, columns, fixData(columns, data), stats, error, updateType, updateCount);
    }

    public QueryResults(
            String id,
            URI infoUri,
            URI partialCancelUri,
            URI nextUri,
            List<Column> columns,
            Iterable<List<Object>> data,
            StatementStats stats,
            QueryError error,
            String updateType,
            Long updateCount)
    {
        this.id = requireNonNull(id, "id is null");
        this.infoUri = requireNonNull(infoUri, "infoUri is null");
        this.partialCancelUri = partialCancelUri;
        this.nextUri = nextUri;
        this.columns = (columns != null) ? ImmutableList.copyOf(columns) : null;
        this.data = (data != null) ? unmodifiableIterable(data) : null;
        this.stats = requireNonNull(stats, "stats is null");
        this.error = error;
        this.updateType = updateType;
        this.updateCount = updateCount;
    }

    @NotNull
    @JsonProperty
    public String getId()
    {
        return id;
    }

    @NotNull
    @JsonProperty
    public URI getInfoUri()
    {
        return infoUri;
    }

    @Nullable
    @JsonProperty
    public URI getPartialCancelUri()
    {
        return partialCancelUri;
    }

    @Nullable
    @JsonProperty
    public URI getNextUri()
    {
        return nextUri;
    }

    @Nullable
    @JsonProperty
    public List<Column> getColumns()
    {
        return columns;
    }

    @Nullable
    @JsonProperty
    public Iterable<List<Object>> getData()
    {
        return data;
    }

    @NotNull
    @JsonProperty
    public StatementStats getStats()
    {
        return stats;
    }

    @Nullable
    @JsonProperty
    public QueryError getError()
    {
        return error;
    }

    @Nullable
    @JsonProperty
    public String getUpdateType()
    {
        return updateType;
    }

    @Nullable
    @JsonProperty
    public Long getUpdateCount()
    {
        return updateCount;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .add("infoUri", infoUri)
                .add("partialCancelUri", partialCancelUri)
                .add("nextUri", nextUri)
                .add("columns", columns)
                .add("hasData", data != null)
                .add("stats", stats)
                .add("error", error)
                .add("updateType", updateType)
                .add("updateCount", updateCount)
                .toString();
    }

    private static Iterable<List<Object>> fixData(List<Column> columns, List<List<Object>> data)
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

        if (signature.getBase().equals(ARRAY)) {
            List<Object> fixedValue = new ArrayList<>();
            for (Object object : List.class.cast(value)) {
                fixedValue.add(fixValue(signature.getTypeParametersAsTypeSignatures().get(0), object));
            }
            return fixedValue;
        }
        if (signature.getBase().equals(MAP)) {
            TypeSignature keySignature = signature.getTypeParametersAsTypeSignatures().get(0);
            TypeSignature valueSignature = signature.getTypeParametersAsTypeSignatures().get(1);
            Map<Object, Object> fixedValue = new HashMap<>();
            for (Map.Entry<?, ?> entry : (Set<Map.Entry<?, ?>>) Map.class.cast(value).entrySet()) {
                fixedValue.put(fixValue(keySignature, entry.getKey()), fixValue(valueSignature, entry.getValue()));
            }
            return fixedValue;
        }
        if (signature.getBase().equals(ROW)) {
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
                String key = namedTypeSignature.getName();
                fixedValue.put(key, fixValue(namedTypeSignature.getTypeSignature(), listValue.get(i)));
            }
            return fixedValue;
        }
        switch (signature.getBase()) {
            case BIGINT:
                if (value instanceof String) {
                    return Long.parseLong((String) value);
                }
                return ((Number) value).longValue();
            case DOUBLE:
                if (value instanceof String) {
                    return Double.parseDouble((String) value);
                }
                return ((Number) value).doubleValue();
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
                return String.class.cast(value);
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
