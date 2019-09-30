package com.facebook.presto.translator;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.LogicalRowExpressions;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.translator.ExpressionTranslator;
import com.facebook.presto.spi.translator.TargetExpression;

import java.util.Map;

public class RowExpressionTranslator<T>
{
    private final RowExpressionTranslatorVisitor<T> visitor;
    private final LogicalRowExpressions logicalRowExpressions;

    public RowExpressionTranslator(
            ExpressionTranslator<T> expressionTranslator, FunctionMetadataManager functionMetadataManager, StandardFunctionResolution functionResolution, DeterminismEvaluator determinismEvaluator)
    {
        this.visitor = new RowExpressionTranslatorVisitor<>(expressionTranslator);
        this.logicalRowExpressions = new LogicalRowExpressions(determinismEvaluator, functionResolution, functionMetadataManager);
    }

    public TargetExpression translate(
            RowExpression expression,
            Map<VariableReferenceExpression, ColumnHandle> assignments)
    {
        return logicalRowExpressions.convertToConjunctiveNormalForm(expression).accept(visitor, assignments);
    }
}
