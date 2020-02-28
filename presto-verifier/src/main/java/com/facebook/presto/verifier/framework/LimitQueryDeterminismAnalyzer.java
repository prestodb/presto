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
package com.facebook.presto.verifier.framework;

import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.verifier.prestoaction.PrestoAction;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.QueryUtil.simpleQuery;
import static com.facebook.presto.verifier.framework.LimitQueryDeterminismAnalysis.DETERMINISTIC;
import static com.facebook.presto.verifier.framework.LimitQueryDeterminismAnalysis.FAILED_DATA_CHANGED;
import static com.facebook.presto.verifier.framework.LimitQueryDeterminismAnalysis.NON_DETERMINISTIC;
import static com.facebook.presto.verifier.framework.LimitQueryDeterminismAnalysis.NOT_RUN;
import static com.facebook.presto.verifier.framework.QueryStage.DETERMINISM_ANALYSIS;
import static com.facebook.presto.verifier.framework.VerifierUtil.callWithQueryStatsConsumer;
import static com.facebook.presto.verifier.framework.VerifierUtil.delimitedIdentifier;
import static com.facebook.presto.verifier.prestoaction.PrestoAction.ResultSetConverter;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Long.parseLong;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class LimitQueryDeterminismAnalyzer
{
    private static final Identifier ROW_NUMBER_COLUMN = delimitedIdentifier("$$row_number");

    private final PrestoAction prestoAction;
    private final boolean enabled;

    private final Statement statement;
    private final long rowCount;
    private final VerificationContext verificationContext;

    public LimitQueryDeterminismAnalyzer(
            PrestoAction prestoAction,
            boolean enabled,
            Statement statement,
            long rowCount,
            VerificationContext verificationContext)
    {
        this.prestoAction = requireNonNull(prestoAction, "prestoAction is null");
        this.enabled = enabled;
        this.statement = requireNonNull(statement, "statement is null");
        checkArgument(rowCount >= 0, "rowCount is negative: %s", rowCount);
        this.rowCount = rowCount;
        this.verificationContext = requireNonNull(verificationContext, "verificationContext is null");
    }

    public LimitQueryDeterminismAnalysis analyze()
    {
        LimitQueryDeterminismAnalysis analysis = analyzeInternal();
        verificationContext.setLimitQueryAnalysis(analysis);
        return analysis;
    }

    private LimitQueryDeterminismAnalysis analyzeInternal()
    {
        if (!enabled) {
            return NOT_RUN;
        }

        Query query;

        // A query is rewritten to either an Insert or a CreateTableAsSelect
        if (statement instanceof Insert) {
            query = ((Insert) statement).getQuery();
        }
        else if (statement instanceof CreateTableAsSelect) {
            query = ((CreateTableAsSelect) statement).getQuery();
        }
        else {
            return NOT_RUN;
        }

        // Flatten TableSubquery
        if (query.getQueryBody() instanceof TableSubquery) {
            Optional<With> with = query.getWith();
            while (query.getQueryBody() instanceof TableSubquery) {
                // ORDER BY and LIMIT must be empty according to syntax
                if (query.getOrderBy().isPresent() || query.getLimit().isPresent()) {
                    return NOT_RUN;
                }
                query = ((TableSubquery) query.getQueryBody()).getQuery();
                // WITH must be empty according to syntax
                if (query.getWith().isPresent()) {
                    return NOT_RUN;
                }
            }
            query = new Query(with, query.getQueryBody(), query.getOrderBy(), query.getLimit());
        }

        if (query.getQueryBody() instanceof QuerySpecification) {
            return analyzeQuerySpecification(query.getWith(), (QuerySpecification) query.getQueryBody());
        }
        return analyzeQuery(query);
    }

    private LimitQueryDeterminismAnalysis analyzeQuery(Query query)
    {
        if (query.getOrderBy().isPresent() || !query.getLimit().isPresent()) {
            return NOT_RUN;
        }
        if (isLimitAll(query.getLimit().get())) {
            return NOT_RUN;
        }
        long limit = parseLong(query.getLimit().get());
        Optional<String> newLimit = Optional.of(Long.toString(limit + 1));
        Query newLimitQuery = new Query(query.getWith(), query.getQueryBody(), Optional.empty(), newLimit);
        return analyzeLimitNoOrderBy(newLimitQuery, limit);
    }

    private LimitQueryDeterminismAnalysis analyzeQuerySpecification(Optional<With> with, QuerySpecification querySpecification)
    {
        if (!querySpecification.getLimit().isPresent()) {
            return NOT_RUN;
        }
        if (isLimitAll(querySpecification.getLimit().get())) {
            return NOT_RUN;
        }
        long limit = parseLong(querySpecification.getLimit().get());
        Optional<String> newLimit = Optional.of(Long.toString(limit + 1));
        Optional<OrderBy> orderBy = querySpecification.getOrderBy();

        if (orderBy.isPresent()) {
            List<SelectItem> selectItems = new ArrayList<>(querySpecification.getSelect().getSelectItems());
            List<ColumnNameOrIndex> orderByKeys = populateSelectItems(selectItems, orderBy.get());
            analyzeLimitOrderBy(
                    new Query(
                            with,
                            new QuerySpecification(
                                    new Select(false, selectItems),
                                    querySpecification.getFrom(),
                                    querySpecification.getWhere(),
                                    querySpecification.getGroupBy(),
                                    querySpecification.getHaving(),
                                    orderBy,
                                    newLimit),
                            Optional.empty(),
                            Optional.empty()),
                    orderByKeys);
//            return analyzeLimitOrderBy(
//                    simpleQuery(
//                            new Select(false, ImmutableList.of(new AllColumns())),
//                            new TableSubquery(new Query(
//                                    with,
//                                    new QuerySpecification(
//                                            new Select(false, columns),
//                                            querySpecification.getFrom(),
//                                            querySpecification.getWhere(),
//                                            querySpecification.getGroupBy(),
//                                            querySpecification.getHaving(),
//                                            orderBy,
//                                            newLimit),
//                                    Optional.empty(),
//                                    Optional.empty())),
//                            new InPredicate(
//                                    ROW_NUMBER_COLUMN,
//                                    new InListExpression(ImmutableList.of(
//                                            new LongLiteral(Long.toString(limit)),
//                                            new LongLiteral(Long.toString(limit + 1)))))),
//                    limit);
        }
        else {
            Query newLimitQuery = new Query(
                    with,
                    new QuerySpecification(
                            querySpecification.getSelect(),
                            querySpecification.getFrom(),
                            querySpecification.getWhere(),
                            querySpecification.getGroupBy(),
                            querySpecification.getHaving(),
                            Optional.empty(),
                            newLimit),
                    Optional.empty(),
                    Optional.empty());
            return analyzeLimitNoOrderBy(newLimitQuery, limit);
        }
    }

    private List<ColumnNameOrIndex> populateSelectItems(List<SelectItem> selectItems, OrderBy orderBy)
    {
        Set<String> aliases = selectItems.stream()
                .filter(item -> item instanceof SingleColumn)
                .map(SingleColumn.class::cast)
                .map(SingleColumn::getAlias)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(Identifier::getValue)
                .collect(toImmutableSet());
        ImmutableList.Builder<ColumnNameOrIndex> orderByKeys = ImmutableList.builder();

        for (int i = 0; i < orderBy.getSortItems().size(); i++) {
            Expression sortKey = orderBy.getSortItems().get(i).getSortKey();
            if (sortKey instanceof LongLiteral) {
                // If sortKey is an long literal, it can be referenced by column index.
                orderByKeys.add(ColumnNameOrIndex.forIndex(((LongLiteral) sortKey).getValue() - 1));
            }
            else if (sortKey instanceof Identifier && aliases.contains(((Identifier) sortKey).getValue())) {
                // If sortKey is an identifier, it can either be an alias or a column name.
                // It is impossible for two columns to have the same alias as sortKey, since otherwise a SYNTAX_ERROR will be thrown due to sortKey being ambiguous.
                // It is possible that sortKey is both an alias and a column name. In that case, sortKey references the aliased column.
                orderByKeys.add(ColumnNameOrIndex.forName(((Identifier) sortKey).getValue()));
            }
            else {
                // If the sortKey is not-alias identifier, select the sortKey column, since it might not be in the selected and it might be aliased.
                // If the sortKey is not an identifier, select the sortKey column.
                String columnName = "$$sort_key$$" + i;
                selectItems.add(new SingleColumn(sortKey, delimitedIdentifier(columnName)));
                orderByKeys.add(ColumnNameOrIndex.forName(columnName));
            }
        }
        return orderByKeys.build();
    }

    private LimitQueryDeterminismAnalysis analyzeLimitNoOrderBy(Query newLimitQuery, long limit)
    {
        Query rowCountQuery = simpleQuery(
                new Select(false, ImmutableList.of(new SingleColumn(new FunctionCall(QualifiedName.of("count"), ImmutableList.of(new LongLiteral("1")))))),
                new TableSubquery(newLimitQuery));

        QueryResult<Long> result = callWithQueryStatsConsumer(
                () -> prestoAction.execute(rowCountQuery, DETERMINISM_ANALYSIS, resultSet -> resultSet.getLong(1)),
                stats -> verificationContext.setLimitQueryAnalysisQueryId(stats.getQueryId()));

        long rowCountHigherLimit = getOnlyElement(result.getResults());
        if (rowCountHigherLimit == rowCount) {
            return DETERMINISTIC;
        }
        if (rowCount >= limit && rowCountHigherLimit > rowCount) {
            return NON_DETERMINISTIC;
        }
        return FAILED_DATA_CHANGED;
    }

    private LimitQueryDeterminismAnalysis analyzeLimitOrderBy(Query tieInspectorQuery, List<ColumnNameOrIndex> orderByKeys)
    {
        QueryResult<List<Object>> result = callWithQueryStatsConsumer(
                () -> prestoAction.execute(tieInspectorQuery, DETERMINISM_ANALYSIS, ResultSetConverter.DEFAULT),
                stats -> verificationContext.setLimitQueryAnalysisQueryId(stats.getQueryId()));
        if (result.getResults().isEmpty()) {
            return FAILED_DATA_CHANGED;
        }
        else if (result.getResults().size() == 1) {
            return DETERMINISTIC;
        }
        else {
            Map<Long, List<Object>> resultByRowNumber = result.getResults().stream()
                    .collect(toImmutableMap(list -> (Long) list.get(0), Functions.identity()));

            List<Object> resultN = resultByRowNumber.get(limit);
            List<Object> resultN1 = resultByRowNumber.get(limit + 1);
            checkState(resultN != null && resultN1 != null, "Invalid result keys: %s", resultByRowNumber.keySet());
            checkState(resultN.size() == resultN1.size(), "Rows have different sizes: %s %s", resultN.size(), resultN1.size());
            for (int i = 1; i < resultN.size(); i++) {
                if (!Objects.equals(resultN.get(i), resultN1.get(i))) {
                    return DETERMINISTIC;
                }
            }
            return NON_DETERMINISTIC;
        }
    }

    private static class TieInspector
            implements ResultSetConverter<List<Object>>
    {
        private final long limit;
        private long row;

        public TieInspector(long limit)
        {
            this.limit = limit;
        }

        List<Object> apply(ResultSet resultSet)
                throws SQLException;
    }

    private static boolean isLimitAll(String limitClause)
    {
        return limitClause.toLowerCase(ENGLISH).equals("all");
    }

    private static List<SingleColumn> orderByToColumns(OrderBy orderBy)
    {
        return orderBy.getSortItems().stream()
                .map(SortItem::getSortKey)
                .map(SingleColumn::new)
                .collect(toImmutableList());
    }

    private static class ColumnNameOrIndex
    {
        private final Optional<String> name;
        private final Optional<Long> index;

        private ColumnNameOrIndex(Optional<String> name, Optional<Long> index)
        {
            checkState(name.isPresent() ^ index.isPresent(), "Exactly one of name and index must be present: %s %s", name, index);
            this.name = requireNonNull(name, "name is null");
            this.index = requireNonNull(index, "index is null");
        }

        public static ColumnNameOrIndex forName(String name)
        {
            return new ColumnNameOrIndex(Optional.of(name), Optional.empty());
        }

        public static ColumnNameOrIndex forIndex(long index)
        {
            return new ColumnNameOrIndex(Optional.empty(), Optional.of(index));
        }
    }
}
