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

import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.expressions.translator.TranslatedExpression.untranslated;
import static com.facebook.presto.expressions.translator.TranslatorAnnotationParser.removeTypeParameters;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class FunctionTranslator<T>
{
    private final Map<FunctionMetadata, MethodHandle> functionMapping;

    public static <T> FunctionTranslator<T> buildFunctionTranslator(Set<Class<?>> translatorContainers)
    {
        ImmutableMap.Builder<FunctionMetadata, MethodHandle> functionMappingBuilder = new ImmutableMap.Builder<>();
        translatorContainers.stream()
                .map(TranslatorAnnotationParser::parseFunctionDefinitions)
                .forEach(functionMappingBuilder::putAll);
        return new FunctionTranslator<>(functionMappingBuilder.build());
    }

    public TranslatedExpression<T> translate(FunctionMetadata functionMetadata, RowExpression original, List<TranslatedExpression<T>> translatedExpressions)
            throws Throwable
    {
        functionMetadata = removeTypeParameters(functionMetadata);
        if (!functionMapping.containsKey(functionMetadata)
                || !translatedExpressions.stream().map(TranslatedExpression::getTranslated).allMatch(Optional::isPresent)) {
            return untranslated(original, translatedExpressions);
        }

        List<T> translatedArguments = translatedExpressions.stream()
                .map(TranslatedExpression::getTranslated)
                .map(Optional::get)
                .collect(toImmutableList());

        return new TranslatedExpression<>(Optional.of((T) functionMapping.get(functionMetadata).invokeWithArguments(translatedArguments)), original, translatedExpressions);
    }

    public TranslatedExpression<T> translate(FunctionMetadata functionMetadata, RowExpression original, TranslatedExpression<T>... translatedArguments)
            throws Throwable
    {
        return translate(functionMetadata, original, ImmutableList.copyOf(translatedArguments));
    }

    public TranslatedExpression<T> translate(FunctionMetadata functionMetadata, RowExpression original)
            throws Throwable
    {
        return translate(functionMetadata, original, ImmutableList.of());
    }

    private FunctionTranslator(Map<FunctionMetadata, MethodHandle> functionMapping)
    {
        this.functionMapping = requireNonNull(functionMapping, "functionMapping is null");
    }
}
