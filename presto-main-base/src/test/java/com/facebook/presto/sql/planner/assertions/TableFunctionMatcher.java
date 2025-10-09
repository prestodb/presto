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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.function.table.Argument;
import com.facebook.presto.spi.function.table.Descriptor;
import com.facebook.presto.spi.function.table.DescriptorArgument;
import com.facebook.presto.spi.function.table.ScalarArgument;
import com.facebook.presto.spi.function.table.TableArgument;
import com.facebook.presto.spi.plan.DataOrganizationSpecification;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableFunctionNode;
import com.facebook.presto.sql.planner.plan.TableFunctionNode.TableArgumentProperties;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.planner.QueryPlanner.toSymbolReferences;
import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.SpecificationProvider.matchSpecification;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class TableFunctionMatcher
        implements Matcher
{
    private final String name;
    private final Map<String, ArgumentValue> arguments;
    private final List<String> properOutputs;
    private final List<List<String>> copartitioningLists;

    private TableFunctionMatcher(
            String name,
            Map<String, ArgumentValue> arguments,
            List<String> properOutputs,
            List<List<String>> copartitioningLists)
    {
        this.name = requireNonNull(name, "name is null");
        this.arguments = ImmutableMap.copyOf(requireNonNull(arguments, "arguments is null"));
        this.properOutputs = ImmutableList.copyOf(requireNonNull(properOutputs, "properOutputs is null"));
        requireNonNull(copartitioningLists, "copartitioningLists is null");
        this.copartitioningLists = copartitioningLists.stream()
                .map(ImmutableList::copyOf)
                .collect(toImmutableList());
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof TableFunctionNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        TableFunctionNode tableFunctionNode = (TableFunctionNode) node;

        if (!name.equals(tableFunctionNode.getName())) {
            return NO_MATCH;
        }

        if (arguments.size() != tableFunctionNode.getArguments().size()) {
            return NO_MATCH;
        }
        for (Map.Entry<String, ArgumentValue> entry : arguments.entrySet()) {
            String name = entry.getKey();
            Argument actual = tableFunctionNode.getArguments().get(name);
            if (actual == null) {
                return NO_MATCH;
            }
            ArgumentValue expected = entry.getValue();
            switch (expected.getType()) {
                case DescriptorArgumentValue.type:
                    DescriptorArgumentValue expectedDescriptor = (DescriptorArgumentValue) expected;
                    if (!(actual instanceof DescriptorArgument) || !expectedDescriptor.getDescriptor().equals(((DescriptorArgument) actual).getDescriptor())) {
                        return NO_MATCH;
                    }
                    break;
                case ScalarArgumentValue.type:
                    ScalarArgumentValue expectedScalar = (ScalarArgumentValue) expected;
                    if (!(actual instanceof ScalarArgument) || !Objects.equals(expectedScalar.getValue(), ((ScalarArgument) actual).getValue())) {
                        return NO_MATCH;
                    }
                    break;
                default:
                    if (!(actual instanceof TableArgument) || getMatchResult(symbolAliases, (TableArgumentValue) expected, tableFunctionNode, name).equals(NO_MATCH)) {
                        return NO_MATCH;
                    }
            }
        }

        if (!ImmutableSet.copyOf(copartitioningLists).equals(ImmutableSet.copyOf(tableFunctionNode.getCopartitioningLists()))) {
            return NO_MATCH;
        }

        ImmutableMap.Builder<String, SymbolReference> properOutputsMapping = ImmutableMap.builder();
        return match(SymbolAliases.builder()
                .putAll(symbolAliases)
                .putAll(properOutputsMapping.buildOrThrow())
                .build());
    }

    private MatchResult getMatchResult(SymbolAliases symbolAliases, TableArgumentValue expected, TableFunctionNode tableFunctionNode, String name)
    {
        TableArgumentValue expectedTableArgument = expected;
        TableArgumentProperties argumentProperties = tableFunctionNode.getTableArgumentProperties().get(expectedTableArgument.sourceIndex());
        if (!name.equals(argumentProperties.getArgumentName())) {
            return NO_MATCH;
        }
        if (expectedTableArgument.rowSemantics() != argumentProperties.isRowSemantics() ||
                expectedTableArgument.pruneWhenEmpty() != argumentProperties.isPruneWhenEmpty() ||
                expectedTableArgument.passThroughColumns() != argumentProperties.getPassThroughSpecification().isDeclaredAsPassThrough()) {
            return NO_MATCH;
        }

        if (expectedTableArgument.specification().isPresent() != argumentProperties.getSpecification().isPresent()) {
            return NO_MATCH;
        }
        if (!expectedTableArgument.specification()
                .map(expectedSpecification -> matchSpecification(argumentProperties.getSpecification().get(), expectedSpecification.getExpectedValue(symbolAliases)))
                .orElse(true)) {
            return NO_MATCH;
        }
        Set<SymbolReference> expectedPassThrough = expectedTableArgument.passThroughVariables().stream()
                .map(symbolAliases::get)
                .collect(toImmutableSet());
        Set<SymbolReference> actualPassThrough = toSymbolReferences(
                argumentProperties.getPassThroughSpecification().getColumns().stream()
                        .map(TableFunctionNode.PassThroughColumn::getOutputVariables)
                        .collect(Collectors.toList()))
                .stream()
                .map(SymbolReference.class::cast)
                .collect(Collectors.toSet());
        if (!expectedPassThrough.equals(actualPassThrough)) {
            return NO_MATCH;
        }
        return match(symbolAliases);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("name", name)
                .add("arguments", arguments)
                .add("properOutputs", properOutputs)
                .add("copartitioningLists", copartitioningLists)
                .toString();
    }

    public static class Builder
    {
        private final PlanMatchPattern[] sources;
        private String name;
        private final ImmutableMap.Builder<String, ArgumentValue> arguments = ImmutableMap.builder();
        private List<String> properOutputs = ImmutableList.of();
        private final ImmutableList.Builder<List<String>> copartitioningLists = ImmutableList.builder();

        Builder(PlanMatchPattern... sources)
        {
            this.sources = Arrays.copyOf(sources, sources.length);
        }

        public Builder name(String name)
        {
            this.name = name;
            return this;
        }

        public Builder addDescriptorArgument(String name, DescriptorArgumentValue descriptor)
        {
            this.arguments.put(name, descriptor);
            return this;
        }

        public Builder addScalarArgument(String name, Object value)
        {
            this.arguments.put(name, new ScalarArgumentValue(value));
            return this;
        }

        public Builder addTableArgument(String name, TableArgumentValue.Builder tableArgument)
        {
            this.arguments.put(name, tableArgument.build());
            return this;
        }

        public Builder properOutputs(List<String> properOutputs)
        {
            this.properOutputs = properOutputs;
            return this;
        }

        public Builder addCopartitioning(List<String> copartitioning)
        {
            this.copartitioningLists.add(copartitioning);
            return this;
        }

        public PlanMatchPattern build()
        {
            return node(TableFunctionNode.class, sources)
                    .with(new TableFunctionMatcher(name, arguments.buildOrThrow(), properOutputs, copartitioningLists.build()));
        }
    }

    interface ArgumentValue
    {
        String getType();
    }

    public static class DescriptorArgumentValue
            implements ArgumentValue
    {
        private final Optional<Descriptor> descriptor;
        public static final String type = "Descriptor";

        public DescriptorArgumentValue(Optional<Descriptor> descriptor)
        {
            this.descriptor = requireNonNull(descriptor, "descriptor is null");
        }

        public static DescriptorArgumentValue descriptorArgument(Descriptor descriptor)
        {
            return new DescriptorArgumentValue(Optional.of(requireNonNull(descriptor, "descriptor is null")));
        }

        public static DescriptorArgumentValue nullDescriptor()
        {
            return new DescriptorArgumentValue(Optional.empty());
        }

        public Optional<Descriptor> getDescriptor()
        {
            return descriptor;
        }

        @Override
        public String getType()
        {
            return type;
        }
    }

    public static class ScalarArgumentValue
            implements ArgumentValue
    {
        private final Object value;
        public static final String type = "Scalar";

        public ScalarArgumentValue(Object value)
        {
            this.value = value;
        }

        public Object getValue()
        {
            return value;
        }

        @Override
        public String getType()
        {
            return type;
        }
    }

    public static class TableArgumentValue
            implements ArgumentValue
    {
        private final int sourceIndex;
        private final boolean rowSemantics;
        private final boolean pruneWhenEmpty;
        private final boolean passThroughColumns;
        private final Optional<ExpectedValueProvider<DataOrganizationSpecification>> specification;
        private final Set<String> passThroughVariables;
        public static final String type = "Table";

        public TableArgumentValue(int sourceIndex, boolean rowSemantics, boolean pruneWhenEmpty, boolean passThroughColumns, Optional<ExpectedValueProvider<DataOrganizationSpecification>> specification, Set<String> passThroughVariables)
        {
            this.sourceIndex = sourceIndex;
            this.rowSemantics = rowSemantics;
            this.pruneWhenEmpty = pruneWhenEmpty;
            this.passThroughColumns = passThroughColumns;
            this.specification = requireNonNull(specification, "specification is null");
            this.passThroughVariables = ImmutableSet.copyOf(passThroughVariables);
        }

        public int sourceIndex()
        {
            return sourceIndex;
        }

        public boolean rowSemantics()
        {
            return rowSemantics;
        }

        public boolean pruneWhenEmpty()
        {
            return pruneWhenEmpty;
        }

        public boolean passThroughColumns()
        {
            return passThroughColumns;
        }

        public Set<String> passThroughVariables()
        {
            return passThroughVariables;
        }

        public Optional<ExpectedValueProvider<DataOrganizationSpecification>> specification()
        {
            return specification;
        }

        @Override
        public String getType()
        {
            return type;
        }

        public static class Builder
        {
            private final int sourceIndex;
            private boolean rowSemantics;
            private boolean pruneWhenEmpty;
            private boolean passThroughColumns;
            private Optional<ExpectedValueProvider<DataOrganizationSpecification>> specification = Optional.empty();
            private Set<String> passThroughVariables = ImmutableSet.of();

            private Builder(int sourceIndex)
            {
                this.sourceIndex = sourceIndex;
            }

            public static Builder tableArgument(int sourceIndex)
            {
                return new Builder(sourceIndex);
            }

            public Builder rowSemantics()
            {
                this.rowSemantics = true;
                this.pruneWhenEmpty = true;
                return this;
            }

            public Builder pruneWhenEmpty()
            {
                this.pruneWhenEmpty = true;
                return this;
            }

            public Builder passThroughColumns()
            {
                this.passThroughColumns = true;
                return this;
            }

            public Builder specification(ExpectedValueProvider<DataOrganizationSpecification> specification)
            {
                this.specification = Optional.of(specification);
                return this;
            }

            public Builder passThroughVariables(Set<String> variables)
            {
                this.passThroughVariables = variables;
                return this;
            }

            private TableArgumentValue build()
            {
                return new TableArgumentValue(sourceIndex, rowSemantics, pruneWhenEmpty, passThroughColumns, specification, passThroughVariables);
            }
        }
    }
}
