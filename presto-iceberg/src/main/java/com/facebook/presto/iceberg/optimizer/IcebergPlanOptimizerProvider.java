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
package com.facebook.presto.iceberg.optimizer;

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.iceberg.IcebergTableProperties;
import com.facebook.presto.iceberg.transaction.IcebergTransactionManager;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorSystemConfig;
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class IcebergPlanOptimizerProvider
        implements ConnectorPlanOptimizerProvider
{
    private final Set<ConnectorPlanOptimizer> planOptimizers;
    private final Set<ConnectorPlanOptimizer> logicalPlanOptimizers;

    @Inject
    public IcebergPlanOptimizerProvider(
            IcebergTransactionManager transactionManager,
            RowExpressionService rowExpressionService,
            StandardFunctionResolution functionResolution,
            FunctionMetadataManager functionMetadataManager,
            IcebergTableProperties tableProperties,
            TypeManager typeManager,
            ConnectorSystemConfig connectorSystemConfig)
    {
        requireNonNull(transactionManager, "transactionManager is null");
        requireNonNull(rowExpressionService, "rowExpressionService is null");
        requireNonNull(functionResolution, "functionResolution is null");
        requireNonNull(functionMetadataManager, "functionMetadataManager is null");
        requireNonNull(typeManager, "typeManager is null");
        requireNonNull(connectorSystemConfig, "connectorSystemConfig is null");

        ImmutableSet.Builder<ConnectorPlanOptimizer> planOptimizerBuilder = ImmutableSet.<ConnectorPlanOptimizer>builder()
                .add(new IcebergPlanOptimizer(functionResolution, rowExpressionService, functionMetadataManager, transactionManager))
                .add(new IcebergFilterPushdown(rowExpressionService, functionResolution, functionMetadataManager, transactionManager, typeManager));
        ImmutableSet.Builder<ConnectorPlanOptimizer> logicalPlanOptimizerBuilder = ImmutableSet.<ConnectorPlanOptimizer>builder()
                .add(new IcebergPlanOptimizer(functionResolution, rowExpressionService, functionMetadataManager, transactionManager))
                .add(new IcebergFilterPushdown(rowExpressionService, functionResolution, functionMetadataManager, transactionManager, typeManager))
                .add(new IcebergAggregationOptimizer(transactionManager, functionResolution))
                .add(new IcebergMetadataOptimizer(functionMetadataManager, typeManager, transactionManager, rowExpressionService, functionResolution));

        // IcebergParquetDereferencePushDown hoists a nested field into a top level column whose name is the
        // flattened subfield path ("msg$_$_$x") while its required subfield keeps the original root ("msg.x").
        // The Java Parquet reader resolves such columns through the subfield path and ignores the column name,
        // but Velox requires the required subfield root to match the column handle name and fails the scan with
        // "Required subfield does not match column name". Native workers get equivalent pruning from
        // PushdownSubfields, which preserves the base column name.
        if (!connectorSystemConfig.isNativeExecution()) {
            planOptimizerBuilder.add(new IcebergParquetDereferencePushDown(transactionManager, rowExpressionService, typeManager, tableProperties));
            logicalPlanOptimizerBuilder.add(new IcebergParquetDereferencePushDown(transactionManager, rowExpressionService, typeManager, tableProperties));
        }

        logicalPlanOptimizerBuilder.add(new IcebergEqualityDeleteAsJoin(functionResolution, transactionManager, typeManager));

        this.planOptimizers = planOptimizerBuilder.build();
        this.logicalPlanOptimizers = logicalPlanOptimizerBuilder.build();
    }

    @Override
    public Set<ConnectorPlanOptimizer> getLogicalPlanOptimizers()
    {
        return logicalPlanOptimizers;
    }

    @Override
    public Set<ConnectorPlanOptimizer> getPhysicalPlanOptimizers()
    {
        return planOptimizers;
    }
}
