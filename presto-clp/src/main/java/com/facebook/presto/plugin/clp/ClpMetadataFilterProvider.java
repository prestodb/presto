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
package com.facebook.presto.plugin.clp;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.plugin.clp.ClpConnectorFactory.CONNECTOR_NAME;
import static com.facebook.presto.plugin.clp.ClpErrorCode.CLP_MANDATORY_METADATA_FILTER_NOT_VALID;
import static com.facebook.presto.plugin.clp.ClpErrorCode.CLP_METADATA_FILTER_CONFIG_NOT_FOUND;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Loads and manages metadata filter configurations for the CLP connector.
 * <p></p>
 * The configuration file is specified by the {@code clp.metadata-filter-config} property
 * and defines metadata filters used to optimize query execution through split pruning.
 * <p></p>
 * Each filter config indicates how a data column--a column in the Presto table--should be mapped to
 * a metadata column--a column in CLP’s metadata database.
 * <p></p>
 * Filter configs can be declared at either a catalog, schema, or table scope. Filter configs under
 * a particular scope will apply to all child scopes (e.g., schema-level filter configs will apply
 * to all tables within that schema).
 * <p></p>
 * Each filter config includes the following fields:
 * <ul>
 *   <li><b>{@code columnName}</b>: the data column's name. Currently, only numeric-type columns can
 *   be used as metadata filters.</li>
 *
 *   <li><b>{@code rangeMapping}</b> <i>(optional)</i>: an object with the following properties:
 *
 *      <br><br>
 *      <b>Note:</b> This option is only valid if the column has a numeric type.
 *
 *      <ul>
 *          <li>{@code lowerBound}: The metadata column that represents the lower bound of values
 *          in a split for the data column.</li>
 *          <li>{@code upperBound}: The metadata column that represents the upper bound of values
 *          in a split for the data column.</li>
 *      </ul>
 *   </li>
 *
 *   <li><b>{@code required}</b> (optional, defaults to {@code false}): indicates whether the
 *   filter must be present in the translated metadata filter SQL query. If a required filter
 *   is missing or cannot be pushed down, the query will be rejected.</li>
 * </ul>
 */
public class ClpMetadataFilterProvider
{
    private final Map<String, List<Filter>> filterMap;

    @Inject
    public ClpMetadataFilterProvider(ClpConfig config)
    {
        requireNonNull(config, "config is null");

        if (null == config.getMetadataFilterConfig()) {
            filterMap = ImmutableMap.of();
            return;
        }
        ObjectMapper mapper = new ObjectMapper();
        try {
            filterMap = mapper.readValue(
                    new File(config.getMetadataFilterConfig()),
                    new TypeReference<Map<String, List<Filter>>>() {});
        }
        catch (IOException e) {
            throw new PrestoException(CLP_METADATA_FILTER_CONFIG_NOT_FOUND, "Failed to metadata filter config file open.");
        }
    }

    public void checkContainsRequiredFilters(SchemaTableName schemaTableName, String metadataFilterSql)
    {
        boolean hasRequiredMetadataFilterColumns = true;
        ImmutableList.Builder<String> notFoundListBuilder = ImmutableList.builder();
        for (String columnName : getRequiredColumnNames(format("%s.%s", CONNECTOR_NAME, schemaTableName))) {
            if (!metadataFilterSql.contains(columnName)) {
                hasRequiredMetadataFilterColumns = false;
                notFoundListBuilder.add(columnName);
            }
        }
        if (!hasRequiredMetadataFilterColumns) {
            throw new PrestoException(
                    CLP_MANDATORY_METADATA_FILTER_NOT_VALID,
                    notFoundListBuilder.build() + " is a mandatory metadata filter column but not valid");
        }
    }

    /**
     * Rewrites the given SQL string to remap filter conditions based on the configured range
     * mappings for the given scope.
     *
     * <p>The {@code scope} follows the format {@code catalog[.schema][.table]}, and determines
     * which filter mappings to apply, since mappings from more specific scopes (e.g., table-level)
     * override or supplement those from broader scopes (e.g., catalog-level). For each scope
     * (catalog, schema, table), this method collects all range mappings defined in the metadata
     * filter configuration.
     *
     * <p>This method performs regex-based replacements to convert numeric filter expressions such
     * as:
     * <ul>
     *   <li>{@code "msg.timestamp" >= 1234} → {@code end_timestamp >= 1234}</li>
     *   <li>{@code "msg.timestamp" <= 5678} → {@code begin_timestamp <= 5678}</li>
     *   <li>{@code "msg.timestamp" = 4567} →
     *   {@code (begin_timestamp <= 4567 AND end_timestamp >= 4567)}</li>
     * </ul>
     *
     * @param scope
     * @param sql
     * @return the rewritten SQL string
     */
    public String remapFilterSql(String scope, String sql)
    {
        String[] splitScope = scope.split("\\.");

        Map<String, RangeMapping> mappings = new HashMap<>(getAllMappingsFromFilters(filterMap.get(splitScope[0])));

        if (1 < splitScope.length) {
            mappings.putAll(getAllMappingsFromFilters(filterMap.get(splitScope[0] + "." + splitScope[1])));
        }

        if (3 == splitScope.length) {
            mappings.putAll(getAllMappingsFromFilters(filterMap.get(scope)));
        }

        String remappedSql = sql;
        for (Map.Entry<String, RangeMapping> entry : mappings.entrySet()) {
            String key = entry.getKey();
            RangeMapping value = entry.getValue();
            remappedSql = remappedSql.replaceAll(
                    format("\"(%s)\"\\s(>=?)\\s([0-9]*)", key),
                    format("%s $2 $3", value.upperBound));
            remappedSql = remappedSql.replaceAll(
                    format("\"(%s)\"\\s(<=?)\\s([0-9]*)", key),
                    format("%s $2 $3", value.lowerBound));
            remappedSql = remappedSql.replaceAll(
                    format("\"(%s)\"\\s(=)\\s([0-9]*)", key),
                    format("(%s <= $3 AND %s >= $3)", value.lowerBound, value.upperBound));
        }
        return remappedSql;
    }

    public Set<String> getColumnNames(String scope)
    {
        return collectColumnNamesFromScopes(scope, this::getAllColumnNamesFromFilters);
    }

    private Set<String> getRequiredColumnNames(String scope)
    {
        return collectColumnNamesFromScopes(scope, this::getRequiredColumnNamesFromFilters);
    }

    private Set<String> collectColumnNamesFromScopes(String scope, Function<List<Filter>, Set<String>> extractor)
    {
        String[] splitScope = scope.split("\\.");
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();

        builder.addAll(extractor.apply(filterMap.get(splitScope[0])));

        if (splitScope.length > 1) {
            builder.addAll(extractor.apply(filterMap.get(splitScope[0] + "." + splitScope[1])));
        }

        if (splitScope.length == 3) {
            builder.addAll(extractor.apply(filterMap.get(scope)));
        }

        return builder.build();
    }

    private Set<String> getAllColumnNamesFromFilters(List<Filter> filters)
    {
        return null != filters ? filters.stream()
                .map(filter -> filter.columnName)
                .collect(toImmutableSet()) : ImmutableSet.of();
    }

    private Set<String> getRequiredColumnNamesFromFilters(List<Filter> filters)
    {
        return null != filters ? filters.stream()
                .filter(filter -> filter.required)
                .map(filter -> filter.columnName)
                .collect(toImmutableSet()) : ImmutableSet.of();
    }

    private Map<String, RangeMapping> getAllMappingsFromFilters(List<Filter> filters)
    {
        return null != filters
                ? filters.stream()
                .filter(filter -> null != filter.rangeMapping)
                .collect(toImmutableMap(
                        filter -> filter.columnName,
                        filter -> filter.rangeMapping))
                : ImmutableMap.of();
    }

    private static class RangeMapping
    {
        @JsonProperty("lowerBound")
        public String lowerBound;

        @JsonProperty("upperBound")
        public String upperBound;

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (!(o instanceof RangeMapping)) {
                return false;
            }
            RangeMapping that = (RangeMapping) o;
            return Objects.equals(lowerBound, that.lowerBound) &&
                    Objects.equals(upperBound, that.upperBound);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(lowerBound, upperBound);
        }
    }

    private static class Filter
    {
        @JsonProperty("columnName")
        public String columnName;

        @JsonProperty("rangeMapping")
        public RangeMapping rangeMapping;

        @JsonProperty("required")
        public boolean required;
    }
}
