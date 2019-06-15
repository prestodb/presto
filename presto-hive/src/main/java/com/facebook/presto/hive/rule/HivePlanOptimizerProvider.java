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
package com.facebook.presto.hive.rule;

import com.facebook.presto.hive.HiveMetadata;
import com.facebook.presto.hive.HivePartitionManager;
import com.facebook.presto.hive.HiveTransactionHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.util.Set;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class HivePlanOptimizerProvider
        implements ConnectorPlanOptimizerProvider
{
    private final Function<HiveTransactionHandle, HiveMetadata> hiveMetadataProvider;
    private final HivePartitionManager partitionManager;
    private final RowExpressionService rowExpressionService;
    private final StandardFunctionResolution functionResolution;

    @Inject
    public HivePlanOptimizerProvider(
            Function<HiveTransactionHandle, HiveMetadata> hiveMetadataProvider,
            HivePartitionManager partitionManager,
            RowExpressionService rowExpressionService,
            StandardFunctionResolution functionResolution)
    {
        this.hiveMetadataProvider = requireNonNull(hiveMetadataProvider, "hiveMetadataProvider is null");
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        this.rowExpressionService = requireNonNull(rowExpressionService, "rowExpressionService is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
    }

    @Override
    public Set<ConnectorPlanOptimizer> getConnectorPlanOptimizers()
    {
        return ImmutableSet.of(new FilterPushdown(hiveMetadataProvider, partitionManager, rowExpressionService, functionResolution));
    }
}
