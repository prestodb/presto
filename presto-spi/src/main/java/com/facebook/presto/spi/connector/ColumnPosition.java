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
package com.facebook.presto.spi.connector;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Position at which a new column should be added to a table.
 * <p>
 * The only implementations are the nested {@link First}, {@link Last} and {@link After}
 * classes. This would be a sealed interface, but the checkstyle version in use cannot
 * parse the {@code sealed} and {@code permits} modifiers.
 */
public interface ColumnPosition
{
    final class First
            implements ColumnPosition
    {
        @Override
        public int hashCode()
        {
            return First.class.hashCode();
        }

        @Override
        public boolean equals(Object obj)
        {
            return (obj != null) && (getClass() == obj.getClass());
        }

        @Override
        public String toString()
        {
            return "FIRST";
        }
    }

    final class Last
            implements ColumnPosition
    {
        @Override
        public int hashCode()
        {
            return Last.class.hashCode();
        }

        @Override
        public boolean equals(Object obj)
        {
            return (obj != null) && (getClass() == obj.getClass());
        }

        @Override
        public String toString()
        {
            return "LAST";
        }
    }

    final class After
            implements ColumnPosition
    {
        private final String columnName;

        public After(String columnName)
        {
            this.columnName = requireNonNull(columnName, "columnName is null");
        }

        public String getColumnName()
        {
            return columnName;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(columnName);
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
            After o = (After) obj;
            return Objects.equals(columnName, o.columnName);
        }

        @Override
        public String toString()
        {
            return "AFTER " + columnName;
        }
    }
}
