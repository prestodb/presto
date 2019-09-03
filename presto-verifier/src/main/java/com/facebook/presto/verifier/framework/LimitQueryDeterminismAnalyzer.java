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
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.verifier.prestoaction.PrestoAction;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static com.facebook.presto.sql.QueryUtil.simpleQuery;
import static com.facebook.presto.verifier.framework.LimitQueryDeterminismAnalyzer.Analysis.DETERMINISTIC;
import static com.facebook.presto.verifier.framework.LimitQueryDeterminismAnalyzer.Analysis.FAILED_DATA_CHANGED;
import static com.facebook.presto.verifier.framework.LimitQueryDeterminismAnalyzer.Analysis.NON_DETERMINISTIC;
import static com.facebook.presto.verifier.framework.LimitQueryDeterminismAnalyzer.Analysis.NOT_RUN;
import static com.facebook.presto.verifier.framework.QueryStage.DETERMINISM_ANALYSIS;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class LimitQueryDeterminismAnalyzer
{
    public enum Analysis
    {
        NOT_RUN,
        NON_DETERMINISTIC,
        DETERMINISTIC,
        FAILED_DATA_CHANGED,
    }

    private final PrestoAction prestoAction;
    private final boolean enabled;

    public LimitQueryDeterminismAnalyzer(PrestoAction prestoAction, VerifierConfig verifierConfig)
    {
        this.prestoAction = requireNonNull(prestoAction, "prestoAction is null");
        this.enabled = verifierConfig.isEnableLimitQueryDeterminismAnalyzer();
    }

    public Analysis analyze(QueryBundle control, long rowCount)
    {
        if (!enabled) {
            return NOT_RUN;
        }

        Statement statement = control.getQuery();
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

        // If Query contains ORDER_BY, we won't be able to make conclusion even if there is LIMIT clause.
        if (query.getOrderBy().isPresent()) {
            return NOT_RUN;
        }

        Query queryNoLimit;
        if (query.getLimit().isPresent()) {
            queryNoLimit = new Query(query.getWith(), query.getQueryBody(), Optional.empty(), Optional.empty());
        }
        else if (query.getQueryBody() instanceof QuerySpecification) {
            QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
            if (querySpecification.getOrderBy().isPresent() || !querySpecification.getLimit().isPresent()) {
                return NOT_RUN;
            }
            queryNoLimit = new Query(
                    query.getWith(),
                    new QuerySpecification(
                            querySpecification.getSelect(),
                            querySpecification.getFrom(),
                            querySpecification.getWhere(),
                            querySpecification.getGroupBy(),
                            querySpecification.getHaving(),
                            Optional.empty(),
                            Optional.empty()),
                    Optional.empty(),
                    Optional.empty());
        }
        else {
            return NOT_RUN;
        }

        Query rowCountQuery = simpleQuery(
                new Select(false, ImmutableList.of(new SingleColumn(new FunctionCall(QualifiedName.of("count"), ImmutableList.of(new LongLiteral("1")))))),
                new TableSubquery(queryNoLimit));
        long rowCountNoLimit = getOnlyElement(prestoAction.execute(rowCountQuery, DETERMINISM_ANALYSIS, resultSet -> resultSet.getLong(1)).getResults());

        if (rowCountNoLimit > rowCount) {
            return NON_DETERMINISTIC;
        }
        if (rowCountNoLimit == rowCount) {
            return DETERMINISTIC;
        }
        return FAILED_DATA_CHANGED;
    }
}
