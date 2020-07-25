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
package com.facebook.presto.sql.planner.planPrinter;

import com.facebook.presto.Session;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Marker;
import com.facebook.presto.common.predicate.Marker.Bound;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.CatalogSchemaTableName;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode.WriterTarget;
import com.facebook.presto.sql.planner.planPrinter.IOPlanPrinter.IOPlan.IOPlanBuilder;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.common.predicate.Marker.Bound.EXACTLY;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class IOPlanPrinter
{
    private final Metadata metadata;
    private final Session session;

    private IOPlanPrinter(Metadata metadata, Session session)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.session = requireNonNull(session, "session is null");
    }

    public static String textIOPlan(PlanNode plan, Metadata metadata, Session session)
    {
        return new IOPlanPrinter(metadata, session).print(plan);
    }

    private String print(PlanNode plan)
    {
        IOPlanBuilder ioPlanBuilder = new IOPlanBuilder();
        plan.accept(new IOPlanVisitor(), ioPlanBuilder);
        return jsonCodec(IOPlan.class).toJson(ioPlanBuilder.build());
    }

    public static class IOPlan
    {
        private final Set<TableColumnInfo> inputTableColumnInfos;
        private final Optional<CatalogSchemaTableName> outputTable;

        @JsonCreator
        public IOPlan(
                @JsonProperty("inputTableColumnInfos") Set<TableColumnInfo> inputTableColumnInfos,
                @JsonProperty("outputTable") Optional<CatalogSchemaTableName> outputTable)
        {
            this.inputTableColumnInfos = ImmutableSet.copyOf(requireNonNull(inputTableColumnInfos, "inputTableColumnInfos is null"));
            this.outputTable = requireNonNull(outputTable, "outputTable is null");
        }

        @JsonProperty
        public Set<TableColumnInfo> getInputTableColumnInfos()
        {
            return inputTableColumnInfos;
        }

        @JsonProperty
        public Optional<CatalogSchemaTableName> getOutputTable()
        {
            return outputTable;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            IOPlan o = (IOPlan) obj;
            return Objects.equals(inputTableColumnInfos, o.inputTableColumnInfos) &&
                    Objects.equals(outputTable, o.outputTable);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(inputTableColumnInfos, outputTable);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("inputTableColumnInfos", inputTableColumnInfos)
                    .add("outputTable", outputTable)
                    .toString();
        }

        protected static class IOPlanBuilder
        {
            private Set<TableColumnInfo> inputTableColumnInfos;
            private Optional<CatalogSchemaTableName> outputTable;

            private IOPlanBuilder()
            {
                this.inputTableColumnInfos = new HashSet<>();
                this.outputTable = Optional.empty();
            }

            private void addInputTableColumnInfo(TableColumnInfo tableColumnInfo)
            {
                inputTableColumnInfos.add(tableColumnInfo);
            }

            private void setOutputTable(CatalogSchemaTableName outputTable)
            {
                this.outputTable = Optional.of(outputTable);
            }

            private IOPlan build()
            {
                return new IOPlan(inputTableColumnInfos, outputTable);
            }
        }

        public static class TableColumnInfo
        {
            private final CatalogSchemaTableName table;
            private final Set<ColumnConstraint> columnConstraints;

            @JsonCreator
            public TableColumnInfo(
                    @JsonProperty("table") CatalogSchemaTableName table,
                    @JsonProperty("columnConstraints") Set<ColumnConstraint> columnConstraints)
            {
                this.table = requireNonNull(table, "table is null");
                this.columnConstraints = requireNonNull(columnConstraints, "columnConstraints is null");
            }

            @JsonProperty
            public CatalogSchemaTableName getTable()
            {
                return table;
            }

            @JsonProperty
            public Set<ColumnConstraint> getColumnConstraints()
            {
                return columnConstraints;
            }

            @Override
            public boolean equals(Object obj)
            {
                if (this == obj) {
                    return true;
                }
                if (obj == null || getClass() != obj.getClass()) {
                    return false;
                }
                TableColumnInfo o = (TableColumnInfo) obj;
                return Objects.equals(table, o.table) &&
                        Objects.equals(columnConstraints, o.columnConstraints);
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(table, columnConstraints);
            }

            @Override
            public String toString()
            {
                return toStringHelper(this)
                        .add("table", table)
                        .add("columnConstraints", columnConstraints)
                        .toString();
            }
        }
    }

    public static class ColumnConstraint
    {
        private final String columnName;
        private final TypeSignature typeSignature;
        private final FormattedDomain domain;

        @JsonCreator
        public ColumnConstraint(
                @JsonProperty("columnName") String columnName,
                @JsonProperty("typeSignature") TypeSignature typeSignature,
                @JsonProperty("domain") FormattedDomain domain)
        {
            this.columnName = requireNonNull(columnName, "columnName is null");
            this.typeSignature = requireNonNull(typeSignature, "type is null");
            this.domain = requireNonNull(domain, "domain is null");
        }

        @JsonProperty
        public String getColumnName()
        {
            return columnName;
        }

        @JsonProperty
        public TypeSignature getTypeSignature()
        {
            return typeSignature;
        }

        @JsonProperty
        public FormattedDomain getDomain()
        {
            return domain;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            ColumnConstraint o = (ColumnConstraint) obj;
            return Objects.equals(columnName, o.columnName) &&
                    Objects.equals(typeSignature, o.typeSignature) &&
                    Objects.equals(domain, o.domain);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(columnName, typeSignature, domain);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("columnName", columnName)
                    .add("typeSignature", typeSignature)
                    .add("domain", domain)
                    .toString();
        }
    }

    public static class FormattedDomain
    {
        private final boolean nullsAllowed;
        private final Set<FormattedRange> ranges;

        @JsonCreator
        public FormattedDomain(
                @JsonProperty("nullsAllowed") boolean nullsAllowed,
                @JsonProperty("ranges") Set<FormattedRange> ranges)
        {
            this.nullsAllowed = nullsAllowed;
            this.ranges = ImmutableSet.copyOf(requireNonNull(ranges, "ranges is null"));
        }

        @JsonProperty
        public boolean isNullsAllowed()
        {
            return nullsAllowed;
        }

        @JsonProperty
        public Set<FormattedRange> getRanges()
        {
            return ranges;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            FormattedDomain o = (FormattedDomain) obj;
            return Objects.equals(nullsAllowed, o.nullsAllowed) &&
                    Objects.equals(ranges, o.ranges);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(nullsAllowed, ranges);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("nullsAllowed", nullsAllowed)
                    .add("ranges", ranges)
                    .toString();
        }
    }

    public static class FormattedRange
    {
        private final FormattedMarker low;
        private final FormattedMarker high;

        @JsonCreator
        public FormattedRange(
                @JsonProperty("low") FormattedMarker low,
                @JsonProperty("high") FormattedMarker high)
        {
            this.low = requireNonNull(low, "low is null");
            this.high = requireNonNull(high, "high is null");
        }

        @JsonProperty
        public FormattedMarker getLow()
        {
            return low;
        }

        @JsonProperty
        public FormattedMarker getHigh()
        {
            return high;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            FormattedRange o = (FormattedRange) obj;
            return Objects.equals(low, o.low) &&
                    Objects.equals(high, o.high);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(low, high);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("low", low)
                    .add("high", high)
                    .toString();
        }
    }

    public static class FormattedMarker
    {
        private final Optional<String> value;
        private final Bound bound;

        @JsonCreator
        public FormattedMarker(
                @JsonProperty("value") Optional<String> value,
                @JsonProperty("bound") Bound bound)
        {
            this.value = requireNonNull(value, "value is null");
            this.bound = requireNonNull(bound, "bound is null");
        }

        @JsonProperty
        public Optional<String> getValue()
        {
            return value;
        }

        @JsonProperty
        public Bound getBound()
        {
            return bound;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            FormattedMarker o = (FormattedMarker) obj;
            return Objects.equals(value, o.value) &&
                    Objects.equals(bound, o.bound);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(value, bound);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("value", value)
                    .add("bound", bound)
                    .toString();
        }
    }

    private class IOPlanVisitor
            extends InternalPlanVisitor<Void, IOPlanBuilder>
    {
        @Override
        public Void visitPlan(PlanNode node, IOPlanBuilder context)
        {
            return processChildren(node, context);
        }

        @Override
        public Void visitTableScan(TableScanNode node, IOPlanBuilder context)
        {
            TableMetadata tableMetadata = metadata.getTableMetadata(session, node.getTable());
            TupleDomain<ColumnHandle> constraints = metadata.toExplainIOConstraints(session, node.getTable(), node.getCurrentConstraint());

            context.addInputTableColumnInfo(new IOPlan.TableColumnInfo(
                    new CatalogSchemaTableName(
                            tableMetadata.getConnectorId().getCatalogName(),
                            tableMetadata.getTable().getSchemaName(),
                            tableMetadata.getTable().getTableName()),
                    parseConstraints(node.getTable(), constraints)));
            return null;
        }

        @Override
        public Void visitTableFinish(TableFinishNode node, IOPlanBuilder context)
        {
            WriterTarget writerTarget = node.getTarget().orElseThrow(() -> new VerifyException("target is absent"));
            context.setOutputTable(new CatalogSchemaTableName(
                    writerTarget.getConnectorId().getCatalogName(),
                    writerTarget.getSchemaTableName().getSchemaName(),
                    writerTarget.getSchemaTableName().getTableName()));
            return processChildren(node, context);
        }

        private Set<ColumnConstraint> parseConstraints(TableHandle tableHandle, TupleDomain<ColumnHandle> constraint)
        {
            checkArgument(!constraint.isNone());
            ImmutableSet.Builder<ColumnConstraint> columnConstraints = ImmutableSet.builder();
            for (Map.Entry<ColumnHandle, Domain> entry : constraint.getDomains().get().entrySet()) {
                ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, tableHandle, entry.getKey());
                columnConstraints.add(new ColumnConstraint(
                        columnMetadata.getName(),
                        columnMetadata.getType().getTypeSignature(),
                        parseDomain(entry.getValue().simplify())));
            }
            return columnConstraints.build();
        }

        private FormattedDomain parseDomain(Domain domain)
        {
            ImmutableSet.Builder<FormattedRange> formattedRanges = ImmutableSet.builder();
            Type type = domain.getType();

            domain.getValues().getValuesProcessor().consume(
                    ranges -> formattedRanges.addAll(
                            ranges.getOrderedRanges().stream()
                                    .map(range -> new FormattedRange(formatMarker(range.getLow()), formatMarker(range.getHigh())))
                                    .collect(toImmutableSet())),
                    discreteValues -> formattedRanges.addAll(
                            discreteValues.getValues().stream()
                                    .map(value -> getVarcharValue(type, value))
                                    .map(value -> new FormattedMarker(Optional.of(value), EXACTLY))
                                    .map(marker -> new FormattedRange(marker, marker))
                                    .collect(toImmutableSet())),
                    allOrNone -> {
                        throw new IllegalStateException("Unreachable AllOrNone consumer");
                    });

            return new FormattedDomain(domain.isNullAllowed(), formattedRanges.build());
        }

        private FormattedMarker formatMarker(Marker marker)
        {
            if (!marker.getValueBlock().isPresent()) {
                return new FormattedMarker(Optional.empty(), marker.getBound());
            }
            return new FormattedMarker(Optional.of(getVarcharValue(marker.getType(), marker.getValue())), marker.getBound());
        }

        private String getVarcharValue(Type type, Object value)
        {
            if (type instanceof VarcharType) {
                return ((Slice) value).toStringUtf8();
            }
            if (type instanceof TinyintType || type instanceof SmallintType || type instanceof IntegerType || type instanceof BigintType) {
                return ((Long) value).toString();
            }
            if (type instanceof BooleanType) {
                return ((Boolean) value).toString();
            }
            throw new PrestoException(NOT_SUPPORTED, format("Unsupported data type in EXPLAIN (TYPE IO): %s", type.getDisplayName()));
        }

        private Void processChildren(PlanNode node, IOPlanBuilder context)
        {
            for (PlanNode child : node.getSources()) {
                child.accept(this, context);
            }

            return null;
        }
    }
}
