package com.facebook.presto.sql.analyzer;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.IdentityHashMap;
import java.util.List;

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

    static Type analyzeExpression(Metadata metadata, TupleDescriptor tupleDescriptor, Analysis analysis, Expression expression)
    {
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(metadata);
        Type type = analyzer.analyze(expression, tupleDescriptor);

        analysis.addFunctionInfos(analyzer.getResolvedFunctions());

        IdentityHashMap<Expression, Type> subExpressions = analyzer.getSubExpressionTypes();

        analysis.addTypes(subExpressions);

        for (Expression subExpression : subExpressions.keySet()) {
            analysis.addResolvedNames(subExpression, analyzer.getResolvedNames());
        }

        return type;
    }

}
