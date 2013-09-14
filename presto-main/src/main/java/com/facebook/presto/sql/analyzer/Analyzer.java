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

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.sql.analyzer.SemanticErrorCode.CANNOT_HAVE_AGGREGATIONS_OR_WINDOWS;
import static com.google.common.base.Preconditions.checkNotNull;

public class Analyzer
{
    private final Metadata metadata;
    private final Session session;
    private final Optional<QueryExplainer> queryExplainer;

    public Analyzer(Session session, Metadata metadata, Optional<QueryExplainer> queryExplainer)
    {
        this.session = checkNotNull(session, "session is null");
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.queryExplainer = checkNotNull(queryExplainer, "query explainer is null");
    }

    public Analysis analyze(Statement statement)
    {
        Analysis analysis = new Analysis();
        StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, session, queryExplainer);
        TupleDescriptor outputDescriptor = analyzer.process(statement, new AnalysisContext());
        analysis.setOutputDescriptor(outputDescriptor);
        return analysis;
    }

    static void verifyNoAggregatesOrWindowFunctions(Metadata metadata, Expression predicate, String clause)
    {
        AggregateExtractor extractor = new AggregateExtractor(metadata);
        extractor.process(predicate, null);

        WindowFunctionExtractor windowExtractor = new WindowFunctionExtractor();
        windowExtractor.process(predicate, null);

        List<FunctionCall> found = ImmutableList.copyOf(Iterables.concat(extractor.getAggregates(), windowExtractor.getWindowFunctions()));

        if (!found.isEmpty()) {
            throw new SemanticException(CANNOT_HAVE_AGGREGATIONS_OR_WINDOWS, predicate, "%s clause cannot contain aggregations or window functions: %s", clause, found);
        }
    }

    static ExpressionAnalysis analyzeExpression(Session session, Metadata metadata, TupleDescriptor tupleDescriptor, Analysis analysis, AnalysisContext context, Expression expression)
    {
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(analysis, session, metadata);
        Type type = analyzer.analyze(expression, tupleDescriptor, context);

        analysis.addFunctionInfos(analyzer.getResolvedFunctions());

        IdentityHashMap<Expression, Type> subExpressions = analyzer.getSubExpressionTypes();

        analysis.addTypes(subExpressions);

        for (Expression subExpression : subExpressions.keySet()) {
            analysis.addResolvedNames(subExpression, analyzer.getResolvedNames());
        }

        Set<InPredicate> subqueryInPredicates = analyzer.getSubqueryInPredicates();

        return new ExpressionAnalysis(type, subqueryInPredicates);
    }

    public static class ExpressionAnalysis
    {
        private final Type type;
        private final Set<InPredicate> subqueryInPredicates;

        public ExpressionAnalysis(Type type, Set<InPredicate> subqueryInPredicates)
        {
            this.type = checkNotNull(type, "type is null");
            this.subqueryInPredicates = ImmutableSet.copyOf(checkNotNull(subqueryInPredicates, "subqueryInPredicates is null"));
        }

        public Type getType()
        {
            return type;
        }

        public Set<InPredicate> getSubqueryInPredicates()
        {
            return subqueryInPredicates;
        }
    }
}
