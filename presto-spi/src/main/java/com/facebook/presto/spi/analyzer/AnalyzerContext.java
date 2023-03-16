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
package com.facebook.presto.spi.analyzer;

import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;

import static java.util.Objects.requireNonNull;

public class AnalyzerContext
{
    private final MetadataResolver metadataResolver;
    private final PlanNodeIdAllocator idAllocator;
    private final VariableAllocator variableAllocator;

    public AnalyzerContext(MetadataResolver metadataResolver, PlanNodeIdAllocator idAllocator, VariableAllocator variableAllocator)
    {
        this.metadataResolver = requireNonNull(metadataResolver, "metadataResolver is null");
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
    }

    public MetadataResolver getMetadataResolver()
    {
        return metadataResolver;
    }

    public PlanNodeIdAllocator getIdAllocator()
    {
        return idAllocator;
    }

    public VariableAllocator getVariableAllocator()
    {
        return variableAllocator;
    }
}
