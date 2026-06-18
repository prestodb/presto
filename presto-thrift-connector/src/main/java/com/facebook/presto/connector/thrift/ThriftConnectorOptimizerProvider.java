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
package com.facebook.presto.connector.thrift;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorSystemConfig;
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.google.common.collect.ImmutableSet;
import jakarta.inject.Inject;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class ThriftConnectorOptimizerProvider
        implements ConnectorPlanOptimizerProvider
{
    private static final Logger log = Logger.get(ThriftConnectorOptimizerProvider.class);
    private final Set<ConnectorPlanOptimizer> planOptimizers;

    @Inject
    public ThriftConnectorOptimizerProvider(
            ThriftMetadata metadata,
            RowExpressionService rowExpressionService,
            ConnectorSystemConfig connectorSystemConfig)
    {
        if (connectorSystemConfig.isNativeExecution()) {
            log.warn("ThriftFilterPushdown added");
            ImmutableSet.Builder<ConnectorPlanOptimizer> planOptimizerBuilder = ImmutableSet.builder();
            planOptimizerBuilder.add(new ThriftFilterPushdown(
                    requireNonNull(metadata, "metadata is null"),
                    requireNonNull(rowExpressionService, "rowExpressionService is null")));
            this.planOptimizers = planOptimizerBuilder.build();
        }
        else {
            this.planOptimizers = ImmutableSet.of();
        }
    }

    @Override
    public Set<ConnectorPlanOptimizer> getLogicalPlanOptimizers()
    {
        return planOptimizers;
    }

    @Override
    public Set<ConnectorPlanOptimizer> getPhysicalPlanOptimizers()
    {
        return planOptimizers;
    }
}
