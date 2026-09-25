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
package com.facebook.presto.delta.rule;

import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorSystemConfig;
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.google.common.collect.ImmutableSet;
import jakarta.inject.Inject;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class DeltaPlanOptimizerProvider
        implements ConnectorPlanOptimizerProvider
{
    private final Set<ConnectorPlanOptimizer> planOptimizers;

    @Inject
    public DeltaPlanOptimizerProvider(RowExpressionService rowExpressionService, ConnectorSystemConfig connectorSystemConfig)
    {
        requireNonNull(connectorSystemConfig, "connectorSystemConfig is null");

        // DeltaParquetDereferencePushDown hoists a nested field into a top level column whose name is the
        // flattened subfield path ("msg$_$_$x") while its required subfield keeps the original root ("msg.x").
        // The Java Parquet reader resolves such columns through the subfield path and ignores the column name,
        // but Velox requires the required subfield root to match the column handle name and fails the scan with
        // "Required subfield does not match column name".
        planOptimizers = connectorSystemConfig.isNativeExecution()
                ? ImmutableSet.of()
                : ImmutableSet.of(new DeltaParquetDereferencePushDown(rowExpressionService));
    }

    @Override
    public Set<ConnectorPlanOptimizer> getLogicalPlanOptimizers()
    {
        return planOptimizers;
    }

    @Override
    public Set<ConnectorPlanOptimizer> getPhysicalPlanOptimizers()
    {
        // New filters may be created in between logical optimization and physical optimization.
        // Push those newly created filters as well.
        return planOptimizers;
    }
}
