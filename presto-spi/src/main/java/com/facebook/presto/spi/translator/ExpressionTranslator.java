package com.facebook.presto.spi.translator;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import java.util.Map;

public interface ExpressionTranslator<T>
{
    TargetExpression<T> translateCallExpression(CallExpression callExpression, Map<VariableReferenceExpression, ColumnHandle> context);

    TargetExpression<T> translateInputReferenceExpression(InputReferenceExpression inputReferenceExpression, Map<VariableReferenceExpression, ColumnHandle> context);

    TargetExpression<T> translateConstantExpression(ConstantExpression constantExpression, Map<VariableReferenceExpression, ColumnHandle> context);

    TargetExpression<T> translateLambdaDefinitionExpression(LambdaDefinitionExpression lambdaDefinitionExpression, Map<VariableReferenceExpression, ColumnHandle> context);

    TargetExpression<T> translateVariableReferenceExpression(VariableReferenceExpression variableReferenceExpression, Map<VariableReferenceExpression, ColumnHandle> context);

    TargetExpression<T> translateIf(SpecialFormExpression specialFormExpression, Map<VariableReferenceExpression, ColumnHandle> context);

    TargetExpression<T> translateNullIf(SpecialFormExpression specialFormExpression, Map<VariableReferenceExpression, ColumnHandle> context);

    TargetExpression<T> translateSwitch(SpecialFormExpression specialFormExpression, Map<VariableReferenceExpression, ColumnHandle> context);

    TargetExpression<T> translateWhen(SpecialFormExpression specialFormExpression, Map<VariableReferenceExpression, ColumnHandle> context);

    TargetExpression<T> translateIsNull(SpecialFormExpression specialFormExpression, Map<VariableReferenceExpression, ColumnHandle> context);

    TargetExpression<T> translateCoalesce(SpecialFormExpression specialFormExpression, Map<VariableReferenceExpression, ColumnHandle> context);

    TargetExpression<T> translateIn(SpecialFormExpression specialFormExpression, Map<VariableReferenceExpression, ColumnHandle> context);

    TargetExpression<T> translateAnd(SpecialFormExpression specialFormExpression, Map<VariableReferenceExpression, ColumnHandle> context);

    TargetExpression<T> translateOr(SpecialFormExpression specialFormExpression, Map<VariableReferenceExpression, ColumnHandle> context);

    TargetExpression<T> translateDereference(SpecialFormExpression specialFormExpression, Map<VariableReferenceExpression, ColumnHandle> context);

    TargetExpression<T> translateRowConstructor(SpecialFormExpression specialFormExpression, Map<VariableReferenceExpression, ColumnHandle> context);

    TargetExpression<T> translateBind(SpecialFormExpression specialFormExpression, Map<VariableReferenceExpression, ColumnHandle> context);
}
