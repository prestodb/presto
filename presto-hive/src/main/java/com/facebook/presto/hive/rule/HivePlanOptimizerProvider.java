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

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HivePartialAggregationPushdown;
import com.facebook.presto.hive.HivePartitionManager;
import com.facebook.presto.hive.HiveTransactionManager;
import com.facebook.presto.hive.TransactionalMetadata;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import java.util.Set;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class HivePlanOptimizerProvider
        implements ConnectorPlanOptimizerProvider
{
    private final Set<ConnectorPlanOptimizer> planOptimizers;

    @Inject
    public HivePlanOptimizerProvider(
            HiveTransactionManager transactionManager,
            RowExpressionService rowExpressionService,
            StandardFunctionResolution functionResolution,
            HivePartitionManager partitionManager,
            FunctionMetadataManager functionMetadataManager,
            TypeManager typeManager,
            Supplier<TransactionalMetadata> metadataFactory)
    {
        requireNonNull(transactionManager, "transactionManager is null");
        requireNonNull(rowExpressionService, "rowExpressionService is null");
        requireNonNull(functionResolution, "functionResolution is null");
        requireNonNull(partitionManager, "partitionManager is null");
        requireNonNull(functionMetadataManager, "functionMetadataManager is null");
        requireNonNull(typeManager, "typeManager is null");
        this.planOptimizers = ImmutableSet.of(
                new HiveFilterPushdown(transactionManager, rowExpressionService, functionResolution, partitionManager, functionMetadataManager),
                new HiveAddRequestedColumnsToLayout(),
                new HiveParquetDereferencePushDown(transactionManager, rowExpressionService),
                new HivePartialAggregationPushdown(functionMetadataManager, functionResolution, metadataFactory));
    }

    @Override
    public Set<ConnectorPlanOptimizer> getLogicalPlanOptimizers()
    {
        return planOptimizers;
    }

    @Override
    public Set<ConnectorPlanOptimizer> getPhysicalPlanOptimizers()
    {
        // New filters may be created in between logical optimization and physical optimization. Push those newly created filters as well.
        return planOptimizers;
    }
}
