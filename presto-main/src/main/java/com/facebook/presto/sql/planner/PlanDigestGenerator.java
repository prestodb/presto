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

import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.Partition;
import com.facebook.presto.metadata.PartitionResult;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.TypeParameter;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.Marker;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.SortedRangeSet;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Window;
import com.facebook.presto.sql.tree.WindowFrame;
import io.airlift.slice.Slice;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Computes digest for a query plan for caching.
 */
public class PlanDigestGenerator
{
    private final SplitManager splitManager;

    @Inject
    public PlanDigestGenerator(SplitManager splitManager)
    {
        this.splitManager = splitManager;
    }

    public PlanDigest generate(Plan plan)
    {
        Visitor visitor = new Visitor();
        PlanDigest digest = plan.getRoot().accept(visitor, null);
        return digest;
    }

    private final class Visitor
            extends PlanVisitor<Void, PlanDigest>
    {
        @Override
        public PlanDigest visitTableScan(TableScanNode node, Void context)
        {
            List<Partition> partitions;
            if (node.getGeneratedPartitions().isPresent()) {
                partitions = node.getGeneratedPartitions().get().getPartitions();
            }
            else {
                PartitionResult allPartitions = splitManager.getPartitions(node.getTable(), Optional.empty());
                partitions = allPartitions.getPartitions();
            }

            Optional<Slice> partitionDigest = splitManager.computeDigest(node.getTable(), partitions);

            if (!partitionDigest.isPresent()) {
                // connector did not generate digest for the partitions, return a random digest
                return generateRandomDigest();
            }

            PlanDigest digest = new PlanDigest();
            digest.update(partitionDigest.get().getBytes());
            updateDigestWithSymbols(digest, node.getOutputSymbols());

            return digest;
        }

        @Override
        public PlanDigest visitFilter(FilterNode node, Void context)
        {
            PlanNode source = node.getSource();
            PlanDigest digest = source.accept(this, null); // visit child

            String predicate = node.getPredicate().toString();
            digest.update(predicate);

            return digest;
        }

        @Override
        public PlanDigest visitProject(ProjectNode node, Void context)
        {
            PlanNode source = node.getSource();
            PlanDigest digest = source.accept(this, null);

            // add assignments to the digest
            Map<Symbol, Expression> assignments = node.getAssignments();
            if (assignments != null && !assignments.isEmpty()) {
                List<Symbol> keys = new ArrayList<>(assignments.keySet());
                Collections.sort(keys);  // order matters for digest
                for (Symbol symbol : keys) {
                    Expression expression = assignments.get(symbol);
                    digest.update(symbol.getName());
                    digest.update(expression.toString());
                }
            }

            // add output symbols to the digest
            updateDigestWithSymbols(digest, node.getOutputSymbols());

            return digest;
        }

        @Override
        public PlanDigest visitAggregation(AggregationNode node, Void context)
        {
            PlanNode source = node.getSource();
            PlanDigest digest = source.accept(this, null);

            updateDigestWithSymbols(digest, node.getGroupBy());

            updateDigestWithSymbolFunctionCallMap(digest, node.getAggregations());
            updateDigestWithSymbolSignatureMap(digest, node.getFunctions());
            updateDigestWithSymbolSymbolMap(digest, node.getMasks());

            digest.update(node.getStep().name());

            updateDigestWithOptionalSymbol(digest, node.getSampleWeight());
            updateDigestWithOptionalSymbol(digest, node.getHashSymbol());

            return digest;
        }

        @Override
        public PlanDigest visitExchange(ExchangeNode node, Void context)
        {
            PlanDigest digest = visitSources(node.getSources());

            digest.update(node.getType().name());

            updateDigestWithSymbols(digest, node.getOutputSymbols());
            updateDigestWithSymbols(digest, node.getPartitionKeys());

            updateDigestWithOptionalSymbol(digest, node.getHashSymbol());

            // list of inputs for each output
            for (List<Symbol> symbols : node.getInputs()) {
                updateDigestWithSymbols(digest, symbols);
            }

            return digest;
        }

        @Override
        public PlanDigest visitOutput(OutputNode node, Void context)
        {
            PlanNode source = node.getSource();
            PlanDigest digest = source.accept(this, null);

            // add columns to the digest
            List<String> columns = new ArrayList<>(node.getColumnNames());
            Collections.sort(columns);
            columns.forEach(digest::update);

            // add outputs to the digest
            updateDigestWithSymbols(digest, node.getOutputSymbols());

            return digest;
        }

        @Override
        public PlanDigest visitLimit(LimitNode node, Void context)
        {
            PlanNode source = node.getSource();
            PlanDigest digest = source.accept(this, null);

            digest.update(String.valueOf(node.getCount()));

            return digest;
        }

        @Override
        public PlanDigest visitDistinctLimit(DistinctLimitNode node, Void context)
        {
            PlanNode source = node.getSource();
            PlanDigest digest = source.accept(this, null);

            digest.update(String.valueOf(node.getLimit()));

            // add symbol for distinct
            updateDigestWithOptionalSymbol(digest, node.getHashSymbol());

            return digest;
        }

        @Override
        public PlanDigest visitTopN(TopNNode node, Void context)
        {
            PlanNode source = node.getSource();
            PlanDigest digest = source.accept(this, null);

            digest.update(String.valueOf(node.getCount()));

            // add order-by symbols
            updateDigestWithSymbols(digest, node.getOrderBy());

            // add sort order map
            updateDigestWithOrderingsMap(digest, node.getOrderings());

            digest.update(String.valueOf(node.isPartial()));

            return digest;
        }

        @Override
        public PlanDigest visitJoin(JoinNode node, Void context)
        {
            PlanNode left = node.getLeft();
            PlanNode right = node.getRight();

            // get digest from the left source, and update it with the digest
            // from the right source.
            PlanDigest digest = left.accept(this, null);
            digest.update(right.accept(this, null));

            // add join clauses to the digest
            for (JoinNode.EquiJoinClause joinClause : node.getCriteria()) {
                digest.update(joinClause.getLeft().getName());
                digest.update(joinClause.getRight().getName());
            }

            updateDigestWithOptionalSymbol(digest, node.getLeftHashSymbol());
            updateDigestWithOptionalSymbol(digest, node.getRightHashSymbol());

            return digest;
        }

        @Override
        public PlanDigest visitSample(SampleNode node, Void context)
        {
            PlanNode source = node.getSource();
            PlanDigest digest = source.accept(this, null);

            digest.update(String.valueOf(node.getSampleRatio()));
            digest.update(node.getSampleType().name());
            digest.update(String.valueOf(node.isRescaled()));

            updateDigestWithOptionalSymbol(digest, node.getSampleWeightSymbol());

            return digest;
        }

        @Override
        public PlanDigest visitValues(ValuesNode node, Void context)
        {
            PlanDigest digest = new PlanDigest();

            updateDigestWithSymbols(digest, node.getOutputSymbols());

            // add rows of expressions to the digest
            for (List<Expression> expressions : node.getRows()) {
                for (Expression expression : expressions) {
                    digest.update(expression.toString());
                }
            }

            return digest;
        }

        @Override
        public PlanDigest visitIndexSource(IndexSourceNode node, Void context)
        {
            PlanDigest digest = new PlanDigest();

            digest.update(node.getIndexHandle().getConnectorId());
            digest.update(node.getTableHandle().getConnectorId());

            List<Symbol> lookupSymbols = new ArrayList<>(node.getLookupSymbols());
            Collections.sort(lookupSymbols);
            updateDigestWithSymbols(digest, lookupSymbols);

            updateDigestWithSymbols(digest, node.getOutputSymbols());

            Map<Symbol, ColumnHandle> assignments = node.getAssignments();
            List<Symbol> keys = new ArrayList<>(assignments.keySet());
            Collections.sort(keys);
            for (Symbol symbol : keys) {
                digest.update(symbol.getName());
                digest.update(assignments.get(symbol).getConnectorId());
            }

            TupleDomain<ColumnHandle> effectiveTupleDomain = node.getEffectiveTupleDomain();
            Map<ColumnHandle, Domain> domains = effectiveTupleDomain.getDomains();
            List<ColumnHandle> columnHandleList = new ArrayList<>(domains.keySet());
            Collections.sort(columnHandleList, (k1, k2) -> k1.getConnectorId().compareTo(k2.getConnectorId()));
            for (ColumnHandle columnHandle : columnHandleList) {
                Domain domain = domains.get(columnHandle);
                digest.update(columnHandle.getConnectorId());
                updateDigestWithDomain(digest, domain);
            }

            return digest;
        }

        @Override
        public PlanDigest visitSemiJoin(SemiJoinNode node, Void context)
        {
            PlanNode source = node.getSource();
            PlanNode filteringSource = node.getFilteringSource();

            // get digest from the source node and update it with digest from filtering source.
            PlanDigest digest = source.accept(this, null);
            digest.update(filteringSource.accept(this, null));

            digest.update(node.getSourceJoinSymbol().getName());
            digest.update(node.getFilteringSourceJoinSymbol().getName());
            digest.update(node.getSemiJoinOutput().getName());

            updateDigestWithOptionalSymbol(digest, node.getSourceHashSymbol());
            updateDigestWithOptionalSymbol(digest, node.getFilteringSourceHashSymbol());

            return digest;
        }

        @Override
        public PlanDigest visitIndexJoin(IndexJoinNode node, Void context)
        {
            PlanNode probeSource = node.getProbeSource();
            PlanNode indexSource = node.getIndexSource();

            // get digest from probe source node and update it with the digest from index source
            PlanDigest digest = probeSource.accept(this, null);
            digest.update(indexSource.accept(this, null));

            digest.update(node.getType().name());

            // add join clause to the digest
            for (IndexJoinNode.EquiJoinClause joinClause : node.getCriteria()) {
                digest.update(joinClause.getProbe().getName());
                digest.update(joinClause.getIndex().getName());
            }

            updateDigestWithOptionalSymbol(digest, node.getProbeHashSymbol());
            updateDigestWithOptionalSymbol(digest, node.getIndexHashSymbol());

            return digest;
        }

        @Override
        public PlanDigest visitSort(SortNode node, Void context)
        {
            PlanNode source = node.getSource();
            PlanDigest digest = source.accept(this, null);

            updateDigestWithSymbols(digest, node.getOrderBy());

            updateDigestWithOrderingsMap(digest, node.getOrderings());

            return digest;
        }

        @Override
        public PlanDigest visitWindow(WindowNode node, Void context)
        {
            PlanNode source = node.getSource();
            PlanDigest digest = source.accept(this, null);

            updateDigestWithSymbols(digest, node.getPartitionBy());
            updateDigestWithSymbols(digest, node.getOrderBy());

            updateDigestWithOrderingsMap(digest, node.getOrderings());

            WindowNode.Frame frame = node.getFrame();
            digest.update(frame.getType().name());
            digest.update(frame.getStartType().name());
            digest.update(frame.getEndType().name());
            updateDigestWithOptionalSymbol(digest, frame.getStartValue());
            updateDigestWithOptionalSymbol(digest, frame.getEndValue());

            updateDigestWithSymbolFunctionCallMap(digest, node.getWindowFunctions());
            updateDigestWithSymbolSignatureMap(digest, node.getSignatures());

            updateDigestWithOptionalSymbol(digest, node.getHashSymbol());

            return digest;
        }

        @Override
        public PlanDigest visitUnion(UnionNode node, Void context)
        {
            PlanDigest digest = visitSources(node.getSources());

            Map<Symbol, Collection<Symbol>> outputToInputsMap = node.getSymbolMapping().asMap();
            List<Symbol> keys = new ArrayList<>(outputToInputsMap.keySet());
            Collections.sort(keys);
            for (Symbol outputSymbol : keys) {
                digest.update(outputSymbol.getName());
                List<Symbol> inputs = new ArrayList<>(outputToInputsMap.get(outputSymbol));
                Collections.sort(inputs);
                for (Symbol inputSymbol : inputs) {
                    digest.update(inputSymbol.getName());
                }
            }

            return digest;
        }

        @Override
        public PlanDigest visitUnnest(UnnestNode node, Void context)
        {
            PlanNode source = node.getSource();
            PlanDigest digest = source.accept(this, null);

            updateDigestWithSymbols(digest, node.getReplicateSymbols());

            Map<Symbol, List<Symbol>> unnestSymbols = node.getUnnestSymbols();
            List<Symbol> keys = new ArrayList<>(unnestSymbols.keySet());
            Collections.sort(keys);
            for (Symbol symbol : keys) {
                digest.update(symbol.getName());
                updateDigestWithSymbols(digest, unnestSymbols.get(symbol));
            }

            return digest;
        }

        @Override
        public PlanDigest visitMarkDistinct(MarkDistinctNode node, Void context)
        {
            PlanNode source = node.getSource();
            PlanDigest digest = source.accept(this, null);

            digest.update(node.getMarkerSymbol().getName());
            updateDigestWithOptionalSymbol(digest, node.getHashSymbol());
            updateDigestWithSymbols(digest, node.getDistinctSymbols());

            return digest;
        }

        @Override
        public PlanDigest visitRowNumber(RowNumberNode node, Void context)
        {
            PlanNode source = node.getSource();
            PlanDigest digest = source.accept(this, null);

            updateDigestWithSymbols(digest, node.getPartitionBy());

            if (node.getMaxRowCountPerPartition().isPresent()) {
                digest.update(String.valueOf(node.getMaxRowCountPerPartition().get().intValue()));
            }

            digest.update(node.getRowNumberSymbol().getName());
            updateDigestWithOptionalSymbol(digest, node.getHashSymbol());

            return digest;
        }

        @Override
        public PlanDigest visitTopNRowNumber(TopNRowNumberNode node, Void context)
        {
            PlanNode source = node.getSource();
            PlanDigest digest = source.accept(this, null);

            updateDigestWithSymbols(digest, node.getPartitionBy());
            updateDigestWithSymbols(digest, node.getOrderBy());
            updateDigestWithOrderingsMap(digest, node.getOrderings());

            digest.update(node.getRowNumberSymbol().getName());
            digest.update(String.valueOf(node.getMaxRowCountPerPartition()));
            digest.update(String.valueOf(node.isPartial()));

            updateDigestWithOptionalSymbol(digest, node.getHashSymbol());

            return digest;
        }

        @Override
        protected PlanDigest visitPlan(PlanNode node, Void context)
        {
            // unsupported plan node, returning a random digest
            return generateRandomDigest();
        }

        /**
         * Helper method to visit multiple sources
         */
        private PlanDigest visitSources(List<PlanNode> sources)
        {
            Iterator<PlanNode> itr = sources.iterator();
            PlanDigest digest = itr.next().accept(this, null);
            while (itr.hasNext()) {
                digest.update(itr.next().accept(this, null));
            }

            return digest;
        }

        private void updateDigestWithSymbols(PlanDigest digest, List<Symbol> symbols)
        {
            if (symbols == null || symbols.isEmpty()) {
                return;
            }

            for (Symbol symbol : symbols) {
                digest.update(symbol.getName());
            }
        }

        private void updateDigestWithDomain(PlanDigest digest, Domain domain)
        {
            SortedRangeSet sortedRangeSet = domain.getRanges();
            digest.update(sortedRangeSet.getType().getName());
            for (Range range : sortedRangeSet) {
                updateDigestWithMarker(digest, range.getLow());
                updateDigestWithMarker(digest, range.getHigh());
            }

            digest.update(String.valueOf(domain.isNullAllowed()));
        }

        private void updateDigestWithMarker(PlanDigest digest, Marker marker)
        {
            digest.update(marker.getType().getName());
            digest.update(marker.getBound().name());
        }

        private void updateDigestWithSymbolSignatureMap(PlanDigest digest, Map<Symbol, Signature> symbolSignatureMap)
        {
            List<Symbol> keys = new ArrayList<>(symbolSignatureMap.keySet());
            Collections.sort(keys);
            for (Symbol symbol : keys) {
                Signature signature = symbolSignatureMap.get(symbol);
                digest.update(symbol.getName());
                updateDigestWithSignature(digest, signature);
            }
        }

        private void updateDigestWithSymbolFunctionCallMap(PlanDigest digest, Map<Symbol, FunctionCall> symbolFunctionCallMap)
        {
            List<Symbol> keys = new ArrayList<>(symbolFunctionCallMap.keySet());
            Collections.sort(keys);
            for (Symbol symbol : keys) {
                FunctionCall functionCall = symbolFunctionCallMap.get(symbol);
                digest.update(symbol.getName());
                updateDigestWithFunctionCall(digest, functionCall);
            }
        }

        private void updateDigestWithSymbolSymbolMap(PlanDigest digest, Map<Symbol, Symbol> symbolSymbolMap)
        {
            List<Symbol> keys = new ArrayList<>(symbolSymbolMap.keySet());
            Collections.sort(keys);
            for (Symbol key : keys) {
                digest.update(key.getName());
                digest.update(symbolSymbolMap.get(key).getName());
            }
        }

        private void updateDigestWithSignature(PlanDigest digest, Signature signature)
        {
            digest.update(signature.getName());

            for (TypeParameter typeParameter : signature.getTypeParameters()) {
                digest.update(typeParameter.getName());
                digest.update(String.valueOf(typeParameter.isComparableRequired()));
                digest.update(String.valueOf(typeParameter.isOrderableRequired()));
                digest.update(typeParameter.getVariadicBound());
            }

            updateDigestWithTypeSignature(digest, signature.getReturnType());

            for (TypeSignature typeSignature : signature.getArgumentTypes()) {
                updateDigestWithTypeSignature(digest, typeSignature);
            }

            digest.update(String.valueOf(signature.isVariableArity()));
            digest.update(String.valueOf(signature.isInternal()));
        }

        private void updateDigestWithTypeSignature(PlanDigest digest, TypeSignature typeSignature)
        {
            digest.update(typeSignature.getBase());

            for (TypeSignature typeSignatureParameter : typeSignature.getParameters()) {
                updateDigestWithTypeSignature(digest, typeSignatureParameter);
            }

            for (Object obj : typeSignature.getLiteralParameters()) {
                digest.update(obj.toString());
            }
        }

        private void updateDigestWithFunctionCall(PlanDigest digest, FunctionCall functionCall)
        {
            for (String part : functionCall.getName().getParts()) {
                digest.update(part);
            }

            if (functionCall.getWindow().isPresent()) {
                updateDigestWithWindow(digest, functionCall.getWindow().get());
            }

            digest.update(String.valueOf(functionCall.isDistinct()));

            for (Expression expression : functionCall.getArguments()) {
                digest.update(expression.toString());
            }
        }

        private void updateDigestWithWindow(PlanDigest digest, Window window)
        {
            for (Expression expression : window.getPartitionBy()) {
                digest.update(expression.toString());
            }

            for (SortItem sortItem : window.getOrderBy()) {
                digest.update(sortItem.getSortKey().toString());
                digest.update(sortItem.getOrdering().name());
                digest.update(sortItem.getNullOrdering().name());
            }

            if (window.getFrame().isPresent()) {
                WindowFrame windowFrame = window.getFrame().get();
                digest.update(windowFrame.getType().name());

                updateDigestWithFrameBound(digest, windowFrame.getStart());
                if (windowFrame.getEnd().isPresent()) {
                    updateDigestWithFrameBound(digest, windowFrame.getEnd().get());
                }

            }

        }

        private void updateDigestWithFrameBound(PlanDigest digest, FrameBound frameBound)
        {
            digest.update(frameBound.getType().name());

            if (frameBound.getValue().isPresent()) {
                digest.update(frameBound.getValue().get().toString());
            }
        }

        private void updateDigestWithOrderingsMap(PlanDigest digest, Map<Symbol, SortOrder> orderings)
        {
            List<Symbol> keys = new ArrayList<>(orderings.keySet());
            Collections.sort(keys);
            for (Symbol symbol : keys) {
                SortOrder sortOder = orderings.get(symbol);
                digest.update(symbol.getName());
                digest.update(String.valueOf(sortOder.isAscending()));
                digest.update(String.valueOf(sortOder.isNullsFirst()));
            }
        }

        private void updateDigestWithOptionalSymbol(PlanDigest digest, Optional<Symbol> symbol)
        {
            if (symbol.isPresent()) {
                digest.update(symbol.get().getName());
            }
        }

        private PlanDigest generateRandomDigest()
        {
            PlanDigest digest = new PlanDigest();
            byte[] buffer = new byte[128];
            ThreadLocalRandom.current().nextBytes(buffer);
            digest.update(buffer);
            return digest;
        }
    }
}
