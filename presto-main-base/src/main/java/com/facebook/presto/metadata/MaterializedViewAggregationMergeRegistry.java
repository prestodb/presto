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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.materializedview.MaterializedViewAggregationMergeStrategy;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class MaterializedViewAggregationMergeRegistry
{
    private final Map<String, MaterializedViewAggregationMergeStrategy> strategies = new ConcurrentHashMap<>();

    public MaterializedViewAggregationMergeRegistry()
    {
        addStrategy(new MaterializedViewAggregationMergeStrategy("count", Optional.of("sum"), false));
        addStrategy(new MaterializedViewAggregationMergeStrategy("sum", Optional.of("sum"), false));
        addStrategy(new MaterializedViewAggregationMergeStrategy("min", Optional.of("min"), true));
        addStrategy(new MaterializedViewAggregationMergeStrategy("max", Optional.of("max"), true));
        addStrategy(new MaterializedViewAggregationMergeStrategy("arbitrary", Optional.empty(), true));
        addStrategy(new MaterializedViewAggregationMergeStrategy("any_value", Optional.empty(), true));
    }

    public synchronized void addStrategies(Iterable<MaterializedViewAggregationMergeStrategy> strategies)
    {
        requireNonNull(strategies, "strategies is null");
        Map<String, MaterializedViewAggregationMergeStrategy> additions = new LinkedHashMap<>();
        for (MaterializedViewAggregationMergeStrategy strategy : strategies) {
            requireNonNull(strategy, "strategy is null");
            String sourceFunctionName = strategy.getSourceFunctionName();
            if (this.strategies.containsKey(sourceFunctionName) || additions.putIfAbsent(sourceFunctionName, strategy) != null) {
                throw new IllegalArgumentException(format("Materialized view aggregation merge strategy is already registered for %s", sourceFunctionName));
            }
        }
        this.strategies.putAll(additions);
    }

    public Optional<MaterializedViewAggregationMergeStrategy> getStrategy(String sourceFunctionName)
    {
        return Optional.ofNullable(strategies.get(canonicalize(sourceFunctionName)));
    }

    public Optional<String> getMergeFunction(String sourceFunctionName)
    {
        return getStrategy(sourceFunctionName).flatMap(MaterializedViewAggregationMergeStrategy::getMergeFunctionName);
    }

    public boolean isDuplicateInsensitive(String sourceFunctionName)
    {
        return getStrategy(sourceFunctionName)
                .map(MaterializedViewAggregationMergeStrategy::isDuplicateInsensitive)
                .orElse(false);
    }

    private void addStrategy(MaterializedViewAggregationMergeStrategy strategy)
    {
        String sourceFunctionName = strategy.getSourceFunctionName();
        if (strategies.putIfAbsent(sourceFunctionName, strategy) != null) {
            throw new IllegalArgumentException(format("Materialized view aggregation merge strategy is already registered for %s", sourceFunctionName));
        }
    }

    private static String canonicalize(String functionName)
    {
        return requireNonNull(functionName, "functionName is null").toLowerCase(Locale.ENGLISH);
    }
}
