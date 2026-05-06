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
package com.facebook.presto.sql.analyzer;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.MaterializedViewStatus;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.Table;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.SystemSessionProperties.isMaterializedViewDataConsistencyEnabled;
import static com.facebook.presto.common.RuntimeMetricName.MANY_PARTITIONS_MISSING_IN_MATERIALIZED_VIEW_COUNT;
import static com.facebook.presto.common.RuntimeMetricName.OPTIMIZED_WITH_MATERIALIZED_VIEW_SUBQUERY_COUNT;
import static com.facebook.presto.common.RuntimeUnit.NONE;
import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.sql.MaterializedViewUtils.ASSOCIATIVE_REWRITE_FUNCTIONS;
import static com.facebook.presto.sql.MaterializedViewUtils.NON_ASSOCIATIVE_REWRITE_FUNCTIONS;
import static com.facebook.presto.sql.MaterializedViewUtils.SUPPORTED_FUNCTION_CALLS;
import static com.facebook.presto.sql.analyzer.MaterializedViewInformationExtractor.MaterializedViewInfo;
import static com.facebook.presto.util.AnalyzerUtil.createParsingOptions;
import static java.util.Objects.requireNonNull;

public class MaterializedViewJoinQueryRewriter
{
    private static final Logger log = Logger.get(MaterializedViewJoinQueryRewriter.class);

    private final Metadata metadata;
    private final Session session;
    private final SqlParser sqlParser;
    private final MetadataResolver metadataResolver;

    public MaterializedViewJoinQueryRewriter(Metadata metadata, Session session, SqlParser sqlParser)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.session = requireNonNull(session, "session is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.metadataResolver = requireNonNull(metadata.getMetadataResolver(session), "metadataResolver is null");
    }

    // TODO: Support cost-based MV selection for JOIN queries (analogous to
    // isMaterializedViewQueryRewriteCostBasedSelectionEnabled in the single-table path).
    // Currently takes the first compatible MV per leaf. Requires changes to
    // QueryWithMVRewriteCandidates / MVRewriteCandidatesNode / SelectLowestCostMVRewrite
    // to handle combinatorial multi-leaf candidates.
    public QuerySpecification tryRewrite(QuerySpecification querySpecification, Relation joinRelation)
    {
        List<JoinLeafInfo> leaves = collectJoinLeaves(joinRelation);

        for (JoinLeafInfo leaf : leaves) {
            QualifiedObjectName tableName = createQualifiedObjectName(session, leaf.table, leaf.table.getName(), metadata);
            List<QualifiedObjectName> referencedMVs = metadata.getReferencedMaterializedViews(session, tableName);

            for (QualifiedObjectName mvName : referencedMVs) {
                QuerySpecification rewritten = new JoinRewriteContext(mvName, leaf).rewrite(querySpecification, joinRelation);
                if (rewritten != querySpecification) {
                    return tryRewrite(rewritten, rewritten.getFrom().get());
                }
            }
        }
        return querySpecification;
    }

    private static List<JoinLeafInfo> collectJoinLeaves(Relation relation)
    {
        ImmutableList.Builder<JoinLeafInfo> leaves = ImmutableList.builder();
        new DefaultTraversalVisitor<Void, Void>()
        {
            @Override
            protected Void visitTable(Table node, Void context)
            {
                leaves.add(new JoinLeafInfo(node, new Identifier(node.getName().toString())));
                return null;
            }

            @Override
            protected Void visitAliasedRelation(AliasedRelation node, Void context)
            {
                if (node.getRelation() instanceof Table) {
                    leaves.add(new JoinLeafInfo((Table) node.getRelation(), node.getAlias()));
                    return null;
                }
                return super.visitAliasedRelation(node, context);
            }
        }.process(relation, null);
        return leaves.build();
    }

    static class JoinLeafInfo
    {
        final Table table;
        final Identifier prefix;

        JoinLeafInfo(Table table, Identifier prefix)
        {
            this.table = requireNonNull(table, "table is null");
            this.prefix = requireNonNull(prefix, "prefix is null");
        }
    }

    private class JoinRewriteContext
    {
        private final QualifiedObjectName materializedViewName;
        private final Identifier swappedPrefix;
        private final Table swappedTable;
        private Table materializedViewTable;
        private Identifier mvPrefix;
        private MaterializedViewInfo mvInfo;
        private MaterializedViewExpressionRewriter expressionRewriter;

        JoinRewriteContext(QualifiedObjectName materializedViewName, JoinLeafInfo swappedLeaf)
        {
            this.materializedViewName = requireNonNull(materializedViewName, "materializedViewName is null");
            this.swappedPrefix = swappedLeaf.prefix;
            this.swappedTable = swappedLeaf.table;
        }

        public QuerySpecification rewrite(QuerySpecification querySpecification, Relation joinRelation)
        {
            try {
                MaterializedViewDefinition mvDef = metadataResolver.getMaterializedView(materializedViewName).orElseThrow(() ->
                        new IllegalStateException("Materialized view definition not present in metadata as expected."));
                materializedViewTable = new Table(QualifiedName.of(mvDef.getSchema(), mvDef.getTable()));
                mvPrefix = isAliasedLeaf() ? swappedPrefix : new Identifier(mvDef.getTable());

                Query mvQuery = (Query) sqlParser.createStatement(mvDef.getOriginalSql(), createParsingOptions(session));
                MaterializedViewInformationExtractor extractor = new MaterializedViewInformationExtractor();
                extractor.process(mvQuery);
                mvInfo = extractor.getMaterializedViewInfo();
                expressionRewriter = new MaterializedViewExpressionRewriter(
                        Optional.of(swappedPrefix), Optional.of(mvPrefix), mvInfo);

                if (!isEligibleForRewrite(querySpecification, joinRelation)) {
                    return querySpecification;
                }

                Relation newFrom = rewriteJoinRelation(joinRelation);
                Select newSelect = rewriteSelect(querySpecification.getSelect());
                Optional<Expression> newWhere = querySpecification.getWhere().map(expressionRewriter::rewriteExpression);
                Optional<GroupBy> newGroupBy = querySpecification.getGroupBy().map(gb ->
                        expressionRewriter.rewriteGroupBy(gb, querySpecification.getSelect().getSelectItems()));
                Optional<OrderBy> newOrderBy = querySpecification.getOrderBy().map(expressionRewriter::rewriteOrderBy);

                if (isMaterializedViewDataConsistencyEnabled(session)) {
                    MaterializedViewStatus status = metadataResolver.getMaterializedViewStatus(materializedViewName, TupleDomain.all());
                    if (!status.isPartiallyMaterialized() && !status.isFullyMaterialized()) {
                        session.getRuntimeStats().addMetricValue(MANY_PARTITIONS_MISSING_IN_MATERIALIZED_VIEW_COUNT, NONE, 1);
                        return querySpecification;
                    }
                }

                session.getRuntimeStats().addMetricValue(OPTIMIZED_WITH_MATERIALIZED_VIEW_SUBQUERY_COUNT, NONE, 1);
                return new QuerySpecification(
                        newSelect,
                        Optional.of(newFrom),
                        newWhere,
                        newGroupBy,
                        querySpecification.getHaving(),
                        newOrderBy,
                        querySpecification.getOffset(),
                        querySpecification.getLimit());
            }
            catch (Exception e) {
                log.warn(e, "Join MV rewrite failed for materialized view %s", materializedViewName);
                return querySpecification;
            }
        }

        private boolean isEligibleForRewrite(QuerySpecification querySpecification, Relation joinRelation)
        {
            if (mvInfo.getWhereClause().isPresent()) {
                log.debug("JOIN MV rewrite rejected: MV has WHERE clause. MV=%s", materializedViewName);
                return false;
            }
            if (querySpecification.getHaving().isPresent()) {
                log.debug("JOIN MV rewrite rejected: query has HAVING. MV=%s", materializedViewName);
                return false;
            }
            if (mvInfo.getGroupBy().isPresent()) {
                if (!querySpecification.getGroupBy().isPresent()) {
                    log.debug("JOIN MV rewrite rejected: MV has GROUP BY but query doesn't. MV=%s", materializedViewName);
                    return false;
                }
                if (!isSafeJoinTypeForGroupByMv(joinRelation)) {
                    log.debug("JOIN MV rewrite rejected: unsafe join type. MV=%s", materializedViewName);
                    return false;
                }
            }
            if (!isGroupByCovered(querySpecification)) {
                log.debug("JOIN MV rewrite rejected: GROUP BY not covered. MV=%s", materializedViewName);
                return false;
            }
            if (!areColumnsCovered(querySpecification, joinRelation)) {
                log.debug("JOIN MV rewrite rejected: columns not covered. MV=%s", materializedViewName);
                return false;
            }
            if (!areAggregatesCompatible(querySpecification)) {
                log.debug("JOIN MV rewrite rejected: aggregates not compatible. MV=%s", materializedViewName);
                return false;
            }
            return true;
        }

        private boolean isAliasedLeaf()
        {
            return !swappedPrefix.equals(new Identifier(swappedTable.getName().toString()));
        }

        // --- Validation ---

        private boolean isGroupByCovered(QuerySpecification querySpecification)
        {
            if (!querySpecification.getGroupBy().isPresent()) {
                return true;
            }
            Optional<Set<Expression>> mvGroupBy = mvInfo.getGroupBy();
            if (!mvGroupBy.isPresent()) {
                return true;
            }
            Map<Expression, Identifier> baseToViewColumnMap = mvInfo.getBaseToViewColumnMap();

            for (GroupingElement element : querySpecification.getGroupBy().get().getGroupingElements()) {
                for (Expression expr : element.getExpressions()) {
                    if (!expressionRewriter.belongsToRewrittenTable(expr)) {
                        continue;
                    }
                    Expression stripped = expressionRewriter.stripPrefix(expr);
                    if (!mvGroupBy.get().contains(stripped) || !baseToViewColumnMap.containsKey(stripped)) {
                        return false;
                    }
                }
            }
            return true;
        }

        private boolean areColumnsCovered(QuerySpecification querySpecification, Relation joinRelation)
        {
            Map<Expression, Identifier> baseToViewColumnMap = mvInfo.getBaseToViewColumnMap();
            AtomicBoolean covered = new AtomicBoolean(true);

            DefaultTraversalVisitor<Void, Void> checker = new DefaultTraversalVisitor<Void, Void>()
            {
                @Override
                protected Void visitFunctionCall(FunctionCall node, Void context)
                {
                    // Skip traversing into aggregate function arguments —
                    // aggregate coverage is checked separately by areAggregatesCompatible
                    if (SUPPORTED_FUNCTION_CALLS.contains(node.getName())) {
                        return null;
                    }
                    return super.visitFunctionCall(node, context);
                }

                @Override
                protected Void visitDereferenceExpression(DereferenceExpression node, Void context)
                {
                    if (expressionRewriter.belongsToRewrittenTable(node)) {
                        if (!baseToViewColumnMap.containsKey(node.getField())) {
                            covered.set(false);
                        }
                    }
                    return null;
                }
            };

            for (SelectItem item : querySpecification.getSelect().getSelectItems()) {
                if (item instanceof SingleColumn) {
                    checker.process(((SingleColumn) item).getExpression(), null);
                }
            }
            querySpecification.getWhere().ifPresent(where -> checker.process(where, null));
            querySpecification.getOrderBy().ifPresent(orderBy ->
                    orderBy.getSortItems().forEach(sortItem -> checker.process(sortItem.getSortKey(), null)));

            new DefaultTraversalVisitor<Void, Void>()
            {
                @Override
                protected Void visitJoin(Join node, Void context)
                {
                    node.getCriteria().ifPresent(criteria -> {
                        if (criteria instanceof JoinOn) {
                            checker.process(((JoinOn) criteria).getExpression(), null);
                        }
                    });
                    return super.visitJoin(node, context);
                }
            }.process(joinRelation, null);

            return covered.get();
        }

        private boolean areAggregatesCompatible(QuerySpecification querySpecification)
        {
            Map<Expression, Identifier> baseToViewColumnMap = mvInfo.getBaseToViewColumnMap();

            for (SelectItem item : querySpecification.getSelect().getSelectItems()) {
                if (!(item instanceof SingleColumn)) {
                    continue;
                }
                Expression expr = ((SingleColumn) item).getExpression();
                if (!(expr instanceof FunctionCall)) {
                    continue;
                }
                FunctionCall funcCall = (FunctionCall) expr;
                if (!SUPPORTED_FUNCTION_CALLS.contains(funcCall.getName())) {
                    continue;
                }

                boolean refRewritten = expressionRewriter.referencesRewrittenTable(funcCall);
                boolean refOther = expressionRewriter.referencesOtherTable(funcCall);

                if (refRewritten && refOther) {
                    return false;
                }
                if (!refRewritten && !refOther && mvInfo.getGroupBy().isPresent()) {
                    if (!baseToViewColumnMap.containsKey(funcCall)) {
                        return false;
                    }
                }
                if (!refRewritten && refOther && mvInfo.getGroupBy().isPresent()) {
                    return false;
                }
                if (refRewritten && !refOther) {
                    if (!ASSOCIATIVE_REWRITE_FUNCTIONS.contains(funcCall.getName())
                            && !NON_ASSOCIATIVE_REWRITE_FUNCTIONS.containsKey(funcCall.getName())) {
                        return false;
                    }
                }
            }
            return true;
        }

        private boolean isSafeJoinTypeForGroupByMv(Relation relation)
        {
            if (relation instanceof Join) {
                Join join = (Join) relation;
                Join.Type type = join.getType();
                if (type == Join.Type.INNER || type == Join.Type.CROSS) {
                    return isSafeJoinTypeForGroupByMv(join.getLeft()) && isSafeJoinTypeForGroupByMv(join.getRight());
                }
                if (type == Join.Type.LEFT) {
                    return !containsSwappedLeaf(join.getLeft()) && isSafeJoinTypeForGroupByMv(join.getRight());
                }
                if (type == Join.Type.RIGHT) {
                    return isSafeJoinTypeForGroupByMv(join.getLeft()) && !containsSwappedLeaf(join.getRight());
                }
                return false;
            }
            return true;
        }

        private boolean containsSwappedLeaf(Relation relation)
        {
            AtomicBoolean found = new AtomicBoolean(false);
            new DefaultTraversalVisitor<Void, Void>()
            {
                @Override
                protected Void visitTable(Table node, Void context)
                {
                    if (swappedPrefix.equals(new Identifier(node.getName().toString()))) {
                        found.set(true);
                    }
                    return null;
                }

                @Override
                protected Void visitAliasedRelation(AliasedRelation node, Void context)
                {
                    if (node.getRelation() instanceof Table && swappedPrefix.equals(node.getAlias())) {
                        found.set(true);
                        return null;
                    }
                    return super.visitAliasedRelation(node, context);
                }
            }.process(relation, null);
            return found.get();
        }

        // --- Join tree rewriting ---

        private Relation rewriteJoinRelation(Relation relation)
        {
            return (Relation) new AstVisitor<Node, Void>()
            {
                @Override
                protected Node visitTable(Table node, Void context)
                {
                    if (swappedPrefix.equals(new Identifier(node.getName().toString()))) {
                        return materializedViewTable;
                    }
                    return node;
                }

                @Override
                protected Node visitAliasedRelation(AliasedRelation node, Void context)
                {
                    if (node.getRelation() instanceof Table && swappedPrefix.equals(node.getAlias())) {
                        return new AliasedRelation(materializedViewTable, node.getAlias(), node.getColumnNames());
                    }
                    Relation newRelation = (Relation) process(node.getRelation());
                    if (newRelation == node.getRelation()) {
                        return node;
                    }
                    return new AliasedRelation(newRelation, node.getAlias(), node.getColumnNames());
                }

                @Override
                protected Node visitJoin(Join node, Void context)
                {
                    Relation newLeft = (Relation) process(node.getLeft());
                    Relation newRight = (Relation) process(node.getRight());
                    Optional<JoinCriteria> newCriteria = node.getCriteria().map(JoinRewriteContext.this::rewriteJoinCriteria);
                    return new Join(node.getType(), newLeft, newRight, newCriteria);
                }

                @Override
                protected Node visitNode(Node node, Void context)
                {
                    return node;
                }
            }.process(relation);
        }

        private JoinCriteria rewriteJoinCriteria(JoinCriteria criteria)
        {
            if (criteria instanceof JoinOn) {
                return new JoinOn(expressionRewriter.rewriteExpression(((JoinOn) criteria).getExpression()));
            }
            return criteria;
        }

        // --- Query component rewriting (delegates to shared expressionRewriter) ---

        private Select rewriteSelect(Select select)
        {
            ImmutableList.Builder<SelectItem> rewrittenItems = ImmutableList.builder();
            for (SelectItem item : select.getSelectItems()) {
                if (item instanceof SingleColumn) {
                    rewrittenItems.add(expressionRewriter.rewriteSingleColumn((SingleColumn) item));
                }
                else {
                    rewrittenItems.add(item);
                }
            }
            return new Select(select.isDistinct(), rewrittenItems.build());
        }
    }
}
