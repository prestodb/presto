package com.facebook.presto.translator;

import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.translator.ExpressionTranslator;
import com.facebook.presto.spi.translator.TargetExpression;
import com.facebook.presto.translator.registry.ConstantTypeRegistry;
import com.facebook.presto.translator.registry.FunctionRegistry;

import java.util.Map;

public abstract class AbstractExpressionTranslator<T>
        implements ExpressionTranslator
{
    private final FunctionRegistry functionRegistry;
    private final ConstantTypeRegistry constantTypeRegistry;

    public AbstractExpressionTranslator(FunctionRegistry functionRegistry, ConstantTypeRegistry constantTypeRegistry)
    {
        this.functionRegistry = functionRegistry;
        this.constantTypeRegistry = constantTypeRegistry;
    }

    @Override
    public TargetExpression<T> translateCallExpression(CallExpression callExpression, Map context)
    {
        return null;
    }

    @Override
    public TargetExpression<T> translateInputReferenceExpression(InputReferenceExpression inputReferenceExpression, Map context)
    {
        return null;
    }

    @Override
    public TargetExpression<T> translateConstantExpression(ConstantExpression constantExpression, Map context)
    {
        return null;
    }

    @Override
    public TargetExpression<T> translateLambdaDefinitionExpression(LambdaDefinitionExpression lambdaDefinitionExpression, Map context)
    {
        return null;
    }

    @Override
    public TargetExpression<T> translateVariableReferenceExpression(VariableReferenceExpression variableReferenceExpression, Map context)
    {
        return null;
    }

    @Override
    public TargetExpression<T> translateIf(SpecialFormExpression specialFormExpression, Map context)
    {
        return null;
    }

    @Override
    public TargetExpression<T> translateNullIf(SpecialFormExpression specialFormExpression, Map context)
    {
        return null;
    }

    @Override
    public TargetExpression<T> translateSwitch(SpecialFormExpression specialFormExpression, Map context)
    {
        return null;
    }

    @Override
    public TargetExpression<T> translateWhen(SpecialFormExpression specialFormExpression, Map context)
    {
        return null;
    }

    @Override
    public TargetExpression<T> translateIsNull(SpecialFormExpression specialFormExpression, Map context)
    {
        return null;
    }

    @Override
    public TargetExpression<T> translateCoalesce(SpecialFormExpression specialFormExpression, Map context)
    {
        return null;
    }

    @Override
    public TargetExpression<T> translateIn(SpecialFormExpression specialFormExpression, Map context)
    {
        return null;
    }

    @Override
    public TargetExpression<T> translateAnd(SpecialFormExpression specialFormExpression, Map context)
    {
        return null;
    }

    @Override
    public TargetExpression<T> translateOr(SpecialFormExpression specialFormExpression, Map context)
    {
        return null;
    }

    @Override
    public TargetExpression<T> translateDereference(SpecialFormExpression specialFormExpression, Map context)
    {
        return null;
    }

    @Override
    public TargetExpression<T> translateRowConstructor(SpecialFormExpression specialFormExpression, Map context)
    {
        return null;
    }

    @Override
    public TargetExpression<T> translateBind(SpecialFormExpression specialFormExpression, Map context)
    {
        return null;
    }
}
