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

import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TranslatedExpression<T>
{
    private final Optional<T> translated;
    private final RowExpression originalExpression;
    private final List<TranslatedExpression<T>> translatedArguments;

    public TranslatedExpression(Optional<T> translated, RowExpression originalExpression, List<TranslatedExpression<T>> translatedArguments)
    {
        this.translated = requireNonNull(translated);
        this.originalExpression = requireNonNull(originalExpression);
        this.translatedArguments = requireNonNull(translatedArguments);
    }

    public Optional<T> getTranslated()
    {
        return translated;
    }

    public RowExpression getOriginalExpression()
    {
        return originalExpression;
    }

    public List<TranslatedExpression<T>> getTranslatedArguments()
    {
        return translatedArguments;
    }

    public static <T> TranslatedExpression<T> untranslated(RowExpression originalExpression)
    {
        return new TranslatedExpression<>(Optional.empty(), originalExpression, ImmutableList.of());
    }

    public static <T> TranslatedExpression<T> untranslated(RowExpression originalExpression, List<TranslatedExpression<T>> translatedArguments)
    {
        return new TranslatedExpression<>(Optional.empty(), originalExpression, translatedArguments);
    }
}
