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
package com.facebook.presto.expressions.translator;

import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import static com.facebook.presto.expressions.translator.TranslatedExpression.untranslated;

public class RowExpressionTranslator<T, C>
{
    public TranslatedExpression<T> translateConstant(ConstantExpression literal, C context, RowExpressionTreeTranslator<T, C> rowExpressionTreeTranslator)
    {
        return untranslated(literal);
    }

    public TranslatedExpression<T> translateVariable(VariableReferenceExpression reference, C context, RowExpressionTreeTranslator<T, C> rowExpressionTreeTranslator)
    {
        return untranslated(reference);
    }

    public TranslatedExpression<T> translateLambda(LambdaDefinitionExpression reference, C context, RowExpressionTreeTranslator<T, C> rowExpressionTreeTranslator)
    {
        return untranslated(reference);
    }

    public TranslatedExpression<T> translateCall(CallExpression call, C context, RowExpressionTreeTranslator<T, C> rowExpressionTreeTranslator)
    {
        return untranslated(call);
    }

    public TranslatedExpression<T> translateSpecialForm(SpecialFormExpression specialForm, C context, RowExpressionTreeTranslator<T, C> rowExpressionTreeTranslator)
    {
        return untranslated(specialForm);
    }
}
