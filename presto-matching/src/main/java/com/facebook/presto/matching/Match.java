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

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class Match<T>
{
    public static <T> Match<T> of(T value, Captures captures)
    {
        return new Match<>(value, captures);
    }

    private final T value;
    private final Captures captures;

    private Match(T value, Captures captures)
    {
        this.value = requireNonNull(value, "value is null");
        this.captures = requireNonNull(captures, "captures is null");
    }

    public T value()
    {
        return value;
    }

    public <S> S capture(Capture<S> capture)
    {
        return captures().get(capture);
    }

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
        Match<?> match = (Match<?>) o;
        return Objects.equals(value, match.value) &&
                Objects.equals(captures, match.captures);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, captures);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("value", value)
                .add("captures", captures)
                .toString();
    }
}
