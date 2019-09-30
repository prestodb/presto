package com.facebook.presto.translator;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.translator.ExpressionTranslator;
import com.facebook.presto.spi.translator.TargetExpression;

import java.util.Map;

import static java.util.Objects.requireNonNull;

class RowExpressionTranslatorVisitor<T>
        implements RowExpressionVisitor<TargetExpression<T>, Map<VariableReferenceExpression, ColumnHandle>>
{
    private final ExpressionTranslator<T> expressionTranslator;

    public RowExpressionTranslatorVisitor(ExpressionTranslator<T> expressionTranslator)
    {
        this.expressionTranslator = requireNonNull(expressionTranslator);
    }

    @Override
    public TargetExpression<T> visitCall(CallExpression call, Map<VariableReferenceExpression, ColumnHandle> context)
    {
        return expressionTranslator.translateCallExpression(call, context);
    }

    @Override
    public TargetExpression<T> visitInputReference(InputReferenceExpression reference, Map<VariableReferenceExpression, ColumnHandle> context)
    {
        return expressionTranslator.translateInputReferenceExpression(reference, context);
    }

    @Override
    public TargetExpression<T> visitConstant(ConstantExpression literal, Map<VariableReferenceExpression, ColumnHandle> context)
    {
        return expressionTranslator.translateConstantExpression(literal, context);
    }

    @Override
    public TargetExpression<T> visitLambda(LambdaDefinitionExpression lambda, Map<VariableReferenceExpression, ColumnHandle> context)
    {
        return expressionTranslator.translateLambdaDefinitionExpression(lambda, context);
    }

    @Override
    public TargetExpression<T> visitVariableReference(VariableReferenceExpression reference, Map<VariableReferenceExpression, ColumnHandle> context)
    {
        return expressionTranslator.translateVariableReferenceExpression(reference, context);
    }

    @Override
    public TargetExpression<T> visitSpecialForm(SpecialFormExpression specialForm, Map<VariableReferenceExpression, ColumnHandle> context)
    {
        switch (specialForm.getForm()) {
            case IF:
                return expressionTranslator.translateIf(specialForm, context);
            case NULL_IF:
                return expressionTranslator.translateNullIf(specialForm, context);
            case SWITCH:
                return expressionTranslator.translateSwitch(specialForm, context);
            case WHEN:
                return expressionTranslator.translateWhen(specialForm, context);
            case IS_NULL:
                return expressionTranslator.translateIsNull(specialForm, context);
            case COALESCE:
                return expressionTranslator.translateCoalesce(specialForm, context);
            case IN:
                return expressionTranslator.translateIn(specialForm, context);
            case AND:
                return expressionTranslator.translateAnd(specialForm, context);
            case OR:
                return expressionTranslator.translateOr(specialForm, context);
            case DEREFERENCE:
                return expressionTranslator.translateDereference(specialForm, context);
            case ROW_CONSTRUCTOR:
                return expressionTranslator.translateRowConstructor(specialForm, context);
            case BIND:
                return expressionTranslator.translateBind(specialForm, context);
            default:
                throw new IllegalStateException("Unexpected value: " + specialForm.getForm());
        }
    }
}
