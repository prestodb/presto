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
import com.facebook.presto.iceberg.optimizer.derivedColumns.IcebergDerivedColumnRewriter;
import com.facebook.presto.iceberg.transaction.IcebergTransactionManager;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.sql.parser.SqlParser;
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
            SqlParser sqlParser)
    {
        requireNonNull(transactionManager, "transactionManager is null");
        requireNonNull(rowExpressionService, "rowExpressionService is null");
        requireNonNull(functionResolution, "functionResolution is null");
        requireNonNull(functionMetadataManager, "functionMetadataManager is null");
        requireNonNull(typeManager, "typeManager is null");
        requireNonNull(sqlParser, "sqlParser is null");
        this.planOptimizers = ImmutableSet.of(
                new IcebergPlanOptimizer(functionResolution, rowExpressionService, functionMetadataManager, transactionManager),
                new IcebergFilterPushdown(rowExpressionService, functionResolution, functionMetadataManager, transactionManager, typeManager),
                new IcebergParquetDereferencePushDown(transactionManager, rowExpressionService, typeManager, tableProperties));
        this.logicalPlanOptimizers = ImmutableSet.of(
                new IcebergPlanOptimizer(functionResolution, rowExpressionService, functionMetadataManager, transactionManager),
                new IcebergFilterPushdown(rowExpressionService, functionResolution, functionMetadataManager, transactionManager, typeManager),
                new IcebergAggregationOptimizer(transactionManager, functionResolution),
                new IcebergMetadataOptimizer(functionMetadataManager, typeManager, transactionManager, rowExpressionService, functionResolution),
                new IcebergParquetDereferencePushDown(transactionManager, rowExpressionService, typeManager, tableProperties),
                new IcebergEqualityDeleteAsJoin(functionResolution, transactionManager, typeManager),
                new IcebergDerivedColumnRewriter(transactionManager, functionMetadataManager, functionResolution, typeManager, sqlParser));
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
