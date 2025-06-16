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
package com.facebook.presto.plugin.clp;

import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.util.Set;

public class ClpPlanOptimizerProvider
        implements ConnectorPlanOptimizerProvider
{
    private final FunctionMetadataManager functionManager;
    private final StandardFunctionResolution functionResolution;

    @Inject
    public ClpPlanOptimizerProvider(FunctionMetadataManager functionManager,
                                    StandardFunctionResolution functionResolution)
    {
        this.functionManager = functionManager;
        this.functionResolution = functionResolution;
    }

    @Override
    public Set<ConnectorPlanOptimizer> getLogicalPlanOptimizers()
    {
        return ImmutableSet.of();
    }

    @Override
    public Set<ConnectorPlanOptimizer> getPhysicalPlanOptimizers()
    {
        return ImmutableSet.of(new ClpPlanOptimizer(functionManager, functionResolution));
    }
}
