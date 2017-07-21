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
package com.facebook.presto.matching;

import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public abstract class Match<T>
{
    public abstract boolean isPresent();

    public abstract T value();

    public final boolean isEmpty()
    {
        return !isPresent();
    }

    public static <S> Match<S> of(S value, Captures captures)
    {
        Captures result;
        requireNonNull(captures);
        return new Match.Present<>(value, captures);
    }

    public static <S> Match<S> empty()
    {
        return new Match.Empty<>();
    }

    public abstract Match<T> filter(Predicate<? super T> predicate);

    public abstract <U> Match<U> map(Function<? super T, ? extends U> mapper);

    public abstract <U> Match<U> flatMap(Function<? super T, Match<U>> mapper);

    public T orElse(T fallback)
    {
        return isPresent() ? value() : fallback;
    }

    public <S> S capture(Capture<S> capture)
    {
        return captures().get(capture);
    }

    public abstract Captures captures();

    private static class Present<T>
            extends Match<T>
    {
        private final T value;
        private final Captures captures;

        private Present(T value, Captures captures)
        {
            this.value = value;
            this.captures = captures;
        }

        @Override
        public boolean isPresent()
        {
            return true;
        }

        @Override
        public T value()
        {
            return value;
        }

        @Override
        public Match<T> filter(Predicate<? super T> predicate)
        {
            return predicate.test(value) ? this : empty();
        }

        @Override
        public <U> Match<U> map(Function<? super T, ? extends U> mapper)
        {
            return Match.of(mapper.apply(value), captures());
        }

        @Override
        public <U> Match<U> flatMap(Function<? super T, Match<U>> mapper)
        {
            return mapper.apply(value);
        }

        @Override
        public Captures captures()
        {
            return captures;
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

            Present<?> present = (Present<?>) o;

            if (value != null ? !value.equals(present.value) : present.value != null) {
                return false;
            }
            return captures.equals(present.captures);
        }

        @Override
        public int hashCode()
        {
            int result = value != null ? value.hashCode() : 0;
            result = 31 * result + captures.hashCode();
            return result;
        }

        @Override
        public String toString()
        {
            return "Match.Present(" +
                    "value=" + value +
                    ", captures=" + captures +
                    ')';
        }
    }

    private static class Empty<T>
            extends Match<T>
    {
        @Override
        public boolean isPresent()
        {
            return false;
        }

        @Override
        public T value()
        {
            throw new NoSuchElementException("Empty match contains no value");
        }

        @Override
        public Match<T> filter(Predicate<? super T> predicate)
        {
            return this;
        }

        @Override
        public <U> Match<U> map(Function<? super T, ? extends U> mapper)
        {
            return empty();
        }

        @Override
        public <U> Match<U> flatMap(Function<? super T, Match<U>> mapper)
        {
            return empty();
        }

        @Override
        public Captures captures()
        {
            throw new NoSuchElementException("Captures are undefined for an empty Match");
        }

        public boolean equals(Object o)
        {
            return this == o || (o != null && getClass() == o.getClass());
        }

        @Override
        public int hashCode()
        {
            return 42;
        }

        @Override
        public String toString()
        {
            return "Match.Empty()";
        }
    }
}
