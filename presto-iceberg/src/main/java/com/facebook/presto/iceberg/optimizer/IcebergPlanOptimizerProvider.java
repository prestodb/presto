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

import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class IcebergPlanOptimizerProvider
        implements ConnectorPlanOptimizerProvider
{
    private final ConnectorPlanOptimizer planOptimizer;

    @Inject
    public IcebergPlanOptimizerProvider(
            ConnectorPlanOptimizer planOptimizer)
    {
        this.planOptimizer = requireNonNull(planOptimizer, "planOptimizer is null");
    }

    @Override
    public Set<ConnectorPlanOptimizer> getLogicalPlanOptimizers()
    {
        return ImmutableSet.of(planOptimizer);
    }

    @Override
    public Set<ConnectorPlanOptimizer> getPhysicalPlanOptimizers()
    {
        return ImmutableSet.of();
    }
}
