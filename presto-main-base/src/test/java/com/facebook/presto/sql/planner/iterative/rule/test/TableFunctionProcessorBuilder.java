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
package com.facebook.presto.sql.planner.iterative.rule.test;

import com.facebook.presto.metadata.TableFunctionHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.function.table.ConnectorTableFunctionHandle;
import com.facebook.presto.spi.plan.DataOrganizationSpecification;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.plan.TableFunctionNode.PassThroughSpecification;
import com.facebook.presto.sql.planner.plan.TableFunctionProcessorNode;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class TableFunctionProcessorBuilder
{
    private String name;
    private List<VariableReferenceExpression> properOutputs = ImmutableList.of();
    private Optional<PlanNode> source = Optional.empty();
    private boolean pruneWhenEmpty;
    private List<PassThroughSpecification> passThroughSpecifications = ImmutableList.of();
    private List<List<VariableReferenceExpression>> requiredSymbols = ImmutableList.of();
    private Optional<Map<VariableReferenceExpression, VariableReferenceExpression>> markerSymbols = Optional.empty();
    private Optional<DataOrganizationSpecification> specification = Optional.empty();
    private Set<VariableReferenceExpression> prePartitioned = ImmutableSet.of();
    private int preSorted;
    private Optional<VariableReferenceExpression> hashSymbol = Optional.empty();
    private ConnectorTableFunctionHandle connectorHandle = new ConnectorTableFunctionHandle() {};

    public TableFunctionProcessorBuilder() {}

    public TableFunctionProcessorBuilder name(String name)
    {
        this.name = name;
        return this;
    }

    public TableFunctionProcessorBuilder properOutputs(VariableReferenceExpression... properOutputs)
    {
        this.properOutputs = ImmutableList.copyOf(properOutputs);
        return this;
    }

    public TableFunctionProcessorBuilder source(PlanNode source)
    {
        this.source = Optional.of(source);
        return this;
    }

    public TableFunctionProcessorBuilder pruneWhenEmpty()
    {
        this.pruneWhenEmpty = true;
        return this;
    }

    public TableFunctionProcessorBuilder passThroughSpecifications(PassThroughSpecification... passThroughSpecifications)
    {
        this.passThroughSpecifications = ImmutableList.copyOf(passThroughSpecifications);
        return this;
    }

    public TableFunctionProcessorBuilder requiredSymbols(List<List<VariableReferenceExpression>> requiredSymbols)
    {
        this.requiredSymbols = requiredSymbols;
        return this;
    }

    public TableFunctionProcessorBuilder markerSymbols(Map<VariableReferenceExpression, VariableReferenceExpression> markerSymbols)
    {
        this.markerSymbols = Optional.of(markerSymbols);
        return this;
    }

    public TableFunctionProcessorBuilder specification(DataOrganizationSpecification specification)
    {
        this.specification = Optional.of(specification);
        return this;
    }

    public TableFunctionProcessorBuilder prePartitioned(Set<VariableReferenceExpression> prePartitioned)
    {
        this.prePartitioned = prePartitioned;
        return this;
    }

    public TableFunctionProcessorBuilder preSorted(int preSorted)
    {
        this.preSorted = preSorted;
        return this;
    }

    public TableFunctionProcessorBuilder hashSymbol(VariableReferenceExpression hashSymbol)
    {
        this.hashSymbol = Optional.of(hashSymbol);
        return this;
    }

    public TableFunctionProcessorBuilder connectorHandle(ConnectorTableFunctionHandle connectorHandle)
    {
        this.connectorHandle = connectorHandle;
        return this;
    }

    public TableFunctionProcessorNode build(PlanNodeIdAllocator idAllocator)
    {
        return new TableFunctionProcessorNode(
                idAllocator.getNextId(),
                name,
                properOutputs,
                source,
                pruneWhenEmpty,
                passThroughSpecifications,
                requiredSymbols,
                markerSymbols,
                specification,
                prePartitioned,
                preSorted,
                hashSymbol,
                new TableFunctionHandle(new ConnectorId("connector_id"), connectorHandle, TestingTransactionHandle.create()));
    }
}
