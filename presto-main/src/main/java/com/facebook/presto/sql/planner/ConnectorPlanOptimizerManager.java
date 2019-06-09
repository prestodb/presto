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
package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Maps.transformValues;
import static java.util.Objects.requireNonNull;

public class ConnectorPlanOptimizerManager
{
    private final Map<ConnectorId, ConnectorPlanOptimizerProvider> planOptimizerProviders = new ConcurrentHashMap<>();

    @Inject
    public ConnectorPlanOptimizerManager() {}

    public void addPlanOptimizerProvider(ConnectorId connectorId, ConnectorPlanOptimizerProvider planOptimizerProvider)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(planOptimizerProvider, "planOptimizerProvider is null");
        checkArgument(planOptimizerProviders.putIfAbsent(connectorId, planOptimizerProvider) == null,
                "ConnectorPlanOptimizerProvider for connector '%s' is already registered", connectorId);
    }

    public Map<ConnectorId, Set<ConnectorPlanOptimizer>> getOptimizers()
    {
        return ImmutableMap.copyOf(transformValues(planOptimizerProviders, ConnectorPlanOptimizerProvider::getConnectorPlanOptimizers));
    }
}
