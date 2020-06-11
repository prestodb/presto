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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.spi.function.RoutineCharacteristics.Determinism.DETERMINISTIC;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Determinism.NOT_DETERMINISTIC;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Language.SQL;
import static com.facebook.presto.spi.function.RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RoutineCharacteristics
{
    public static class Language
    {
        public static final Language SQL = new Language("SQL");

        private final String language;

        @JsonCreator
        public Language(String language)
        {
            this.language = requireNonNull(language.toUpperCase());
        }

        @JsonValue
        public String getLanguage()
        {
            return language;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(language);
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
            Language that = (Language) o;
            return Objects.equals(language, that.language);
        }

        @Override
        public String toString()
        {
            return language;
        }
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

    @JsonCreator
    public RoutineCharacteristics(
            @JsonProperty("language") Optional<Language> language,
            @JsonProperty("determinism") Optional<Determinism> determinism,
            @JsonProperty("nullCallClause") Optional<NullCallClause> nullCallClause)
    {
        this.language = language.orElse(SQL);
        this.determinism = determinism.orElse(NOT_DETERMINISTIC);
        this.nullCallClause = nullCallClause.orElse(CALLED_ON_NULL_INPUT);
    }

    @JsonProperty
    public Language getLanguage()
    {
        return language;
    }

    @JsonProperty
    public Determinism getDeterminism()
    {
        return determinism;
    }

    @JsonProperty
    public NullCallClause getNullCallClause()
    {
        return nullCallClause;
    }

    public boolean isDeterministic()
    {
        return determinism == DETERMINISTIC;
    }

    public boolean isCalledOnNullInput()
    {
        return nullCallClause == CALLED_ON_NULL_INPUT;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(RoutineCharacteristics routineCharacteristics)
    {
        return new Builder(routineCharacteristics);
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
        return Objects.equals(language, that.language)
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

    public static class Builder
    {
        private Language language;
        private Determinism determinism;
        private NullCallClause nullCallClause;

        private Builder() {}

        private Builder(RoutineCharacteristics routineCharacteristics)
        {
            this.language = routineCharacteristics.getLanguage();
            this.determinism = routineCharacteristics.getDeterminism();
            this.nullCallClause = routineCharacteristics.getNullCallClause();
        }

        public Builder setLanguage(Language language)
        {
            this.language = requireNonNull(language, "language is null");
            return this;
        }

        public Builder setDeterminism(Determinism determinism)
        {
            this.determinism = requireNonNull(determinism, "determinism is null");
            return this;
        }

        public Builder setNullCallClause(NullCallClause nullCallClause)
        {
            this.nullCallClause = requireNonNull(nullCallClause, "nullCallClause is null");
            return this;
        }

        public RoutineCharacteristics build()
        {
            return new RoutineCharacteristics(
                    Optional.ofNullable(language),
                    Optional.ofNullable(determinism),
                    Optional.ofNullable(nullCallClause));
        }
    }
}
