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
package com.facebook.presto.plugin.clp.split.filter;

import com.facebook.presto.plugin.clp.ClpConfig;
import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.plugin.clp.ClpErrorCode.CLP_MANDATORY_SPLIT_FILTER_NOT_VALID;
import static com.facebook.presto.plugin.clp.ClpErrorCode.CLP_SPLIT_FILTER_CONFIG_NOT_FOUND;
import static com.facebook.presto.plugin.clp.split.filter.ClpSplitFilterConfig.CustomSplitFilterOptions;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

/**
 * Loads and manages {@link ClpSplitFilterConfig}s from a config file.
 * <p></p>
 * The config file is specified by the {@code clp.split-filter-config} property.
 * <p></p>
 * Filter configs can be declared at either a catalog, schema, or table scope. Filter configs under
 * a particular scope will apply to all child scopes (e.g., schema-level filter configs will apply
 * to all tables within that schema).
 * <p></p>
 * Implementations of this class can customize filter configs through the {@code "customOptions"}
 * field within each {@link ClpSplitFilterConfig}.
 */
public abstract class ClpSplitFilterProvider
{
    protected final Map<String, List<ClpSplitFilterConfig>> filterMap;

    public ClpSplitFilterProvider(ClpConfig config)
    {
        requireNonNull(config, "config is null");

        if (null == config.getSplitFilterConfig()) {
            filterMap = ImmutableMap.of();
            return;
        }
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(
                CustomSplitFilterOptions.class,
                new ClpSplitFilterConfigCustomOptionsDeserializer(getCustomSplitFilterOptionsClass()));
        mapper.registerModule(module);
        try {
            filterMap = mapper.readValue(
                    new File(config.getSplitFilterConfig()),
                    new TypeReference<Map<String, List<ClpSplitFilterConfig>>>() {});
        }
        catch (IOException e) {
            throw new PrestoException(CLP_SPLIT_FILTER_CONFIG_NOT_FOUND, "Failed to open split filter config file", e);
        }
    }

    /**
     * Rewrites {@code pushDownExpression} to remap filter conditions based on the
     * {@code "customOptions"} for the given scope.
     * <p></p>
     * {@code scope} follows the format {@code catalog[.schema][.table]}, and determines which
     * filter mappings to apply, since mappings from more specific scopes (e.g., table-level)
     * override or supplement those from broader scopes (e.g., catalog-level). For each scope
     * (catalog, schema, table), this method collects all mappings defined in
     * {@code "customOptions"}.
     *
     * @param scope the scope of the filter
     * @param pushDownExpression the expression to be rewritten
     * @return the rewritten expression
     */
    public abstract String remapSplitFilterPushDownExpression(String scope, String pushDownExpression);

    /**
     * Checks for the given table, if {@code splitFilterPushDownExpression} contains all required
     * fields.
     *
     * @param tableScopeSet the set of scopes of the tables that are being queried
     * @param splitFilterPushDownExpression the expression to be checked
     */
    public void checkContainsRequiredFilters(Set<String> tableScopeSet, String splitFilterPushDownExpression)
    {
        boolean hasRequiredSplitFilterColumns = true;
        ImmutableList.Builder<String> notFoundListBuilder = ImmutableList.builder();
        for (String tableScope : tableScopeSet) {
            for (String columnName : getRequiredColumnNames(tableScope)) {
                if (!splitFilterPushDownExpression.contains(columnName)) {
                    hasRequiredSplitFilterColumns = false;
                    notFoundListBuilder.add(columnName);
                }
            }
        }
        if (!hasRequiredSplitFilterColumns) {
            throw new PrestoException(
                    CLP_MANDATORY_SPLIT_FILTER_NOT_VALID,
                    notFoundListBuilder.build() + " is a mandatory split filter column but not valid");
        }
    }

    public Set<String> getColumnNames(String scope)
    {
        return collectColumnNamesFromScopes(scope, this::getAllColumnNamesFromFilters);
    }

    /**
     * Returns the implemented {@link CustomSplitFilterOptions} class. To respect our code style, we
     * recommend implementing a {@code protected static class} as an inner class in the implemented
     * {@link ClpSplitFilterProvider} class.
     *
     * @return the implemented {@link CustomSplitFilterOptions} class
     */
    protected abstract Class<? extends CustomSplitFilterOptions> getCustomSplitFilterOptionsClass();

    private Set<String> getRequiredColumnNames(String scope)
    {
        return collectColumnNamesFromScopes(scope, this::getRequiredColumnNamesFromFilters);
    }

    private Set<String> collectColumnNamesFromScopes(String scope, Function<List<ClpSplitFilterConfig>, Set<String>> extractor)
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

    private Set<String> getAllColumnNamesFromFilters(List<ClpSplitFilterConfig> filters)
    {
        return null != filters ? filters.stream()
                .map(filter -> filter.columnName)
                .collect(toImmutableSet()) : ImmutableSet.of();
    }

    private Set<String> getRequiredColumnNamesFromFilters(List<ClpSplitFilterConfig> filters)
    {
        return null != filters ? filters.stream()
                .filter(filter -> filter.required)
                .map(filter -> filter.columnName)
                .collect(toImmutableSet()) : ImmutableSet.of();
    }
}
