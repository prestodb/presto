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
package com.facebook.presto.spi.function;

import java.util.Objects;

import static com.facebook.presto.spi.function.RoutineCharacteristics.Determinism.DETERMINISTIC;
import static com.facebook.presto.spi.function.RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RoutineCharacteristics
{
    public enum Language
    {
        SQL;
    }

    public enum Determinism
    {
        DETERMINISTIC,
        NOT_DETERMINISTIC;
    }

    public enum NullCallClause
    {
        RETURNS_NULL_ON_NULL_INPUT,
        CALLED_ON_NULL_INPUT;
    }

    private final Language language;
    private final Determinism determinism;
    private final NullCallClause nullCallClause;

    public RoutineCharacteristics(
            Language language,
            Determinism determinism,
            NullCallClause nullCallClause)
    {
        this.language = requireNonNull(language, "language is null");
        this.determinism = requireNonNull(determinism, "determinism is null");
        this.nullCallClause = requireNonNull(nullCallClause, "nullCallClause is null");
    }

    public Language getLanguage()
    {
        return language;
    }

    public boolean isDeterministic()
    {
        return determinism == DETERMINISTIC;
    }

    public boolean isCalledOnNullInput()
    {
        return nullCallClause == CALLED_ON_NULL_INPUT;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RoutineCharacteristics that = (RoutineCharacteristics) o;
        return language == that.language
                && determinism == that.determinism
                && nullCallClause == that.nullCallClause;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(language, determinism, nullCallClause);
    }

    @Override
    public String toString()
    {
        return format("(%s, %s, %s)", language, determinism, nullCallClause);
    }
}
