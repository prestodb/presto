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
package com.facebook.presto.verifier.checksum;

import com.facebook.presto.verifier.framework.Column;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ColumnMatchResult<T extends ColumnChecksum>
{
    private final boolean matched;
    private final Column column;
    private final Optional<String> message;
    private final T controlColumnChecksum;
    private final T testColumnChecksum;

    public ColumnMatchResult(boolean matched, Column column, T controlColumnChecksum, T testColumnChecksum)
    {
        this(matched, column, Optional.empty(), controlColumnChecksum, testColumnChecksum);
    }

    public ColumnMatchResult(boolean matched, Column column, Optional<String> message, T controlColumnChecksum, T testColumnChecksum)
    {
        this.matched = matched;
        this.column = requireNonNull(column, "column is null");
        this.message = requireNonNull(message, "message is null");
        this.controlColumnChecksum = requireNonNull(controlColumnChecksum, "controlColumnChecksum is null");
        this.testColumnChecksum = requireNonNull(testColumnChecksum, "testColumnChecksum is null");
    }

    public boolean isMatched()
    {
        return matched;
    }

    public Column getColumn()
    {
        return column;
    }

    public Optional<String> getMessage()
    {
        return message;
    }

    public T getControlChecksum()
    {
        return controlColumnChecksum;
    }

    public T getTestChecksum()
    {
        return testColumnChecksum;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ColumnMatchResult<?> o = (ColumnMatchResult<?>) obj;
        return Objects.equals(matched, o.matched) &&
                Objects.equals(column, o.column) &&
                Objects.equals(controlColumnChecksum, o.controlColumnChecksum) &&
                Objects.equals(testColumnChecksum, o.testColumnChecksum);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(column, controlColumnChecksum, testColumnChecksum);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("matched", matched)
                .add("column", column)
                .add("controlColumnChecksum", controlColumnChecksum)
                .add("testColumnChecksum", testColumnChecksum)
                .toString();
    }
}
