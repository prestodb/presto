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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.sql.planner.Symbol;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static java.util.Objects.requireNonNull;

@Immutable
public abstract class SetOperationNode
        extends PlanNode
{
    private final MultiSourceSymbolMapping multiSourceSymbolMapping;

    @JsonCreator
    protected SetOperationNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("multiSourceSymbolMapping") MultiSourceSymbolMapping multiSourceSymbolMapping)
    {
        super(id);
        this.multiSourceSymbolMapping = requireNonNull(multiSourceSymbolMapping, "multiSourceSymbolMapping is null");
    }

    @Override
    @JsonProperty("sources")
    public List<PlanNode> getSources()
    {
        return multiSourceSymbolMapping.getSources();
    }

    @Override
    @JsonProperty("outputs")
    public List<Symbol> getOutputSymbols()
    {
        return multiSourceSymbolMapping.getOutputSymbols();
    }

    @JsonProperty("multiSourceSymbolMapping")
    public MultiSourceSymbolMapping getMultiSourceSymbolMapping()
    {
        return multiSourceSymbolMapping;
    }
}
