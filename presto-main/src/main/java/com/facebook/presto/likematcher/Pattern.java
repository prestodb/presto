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
package com.facebook.presto.likematcher;

import com.google.common.base.Strings;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

public interface Pattern
{
    class Literal
            implements Pattern
    {
        private final String value;

        public Literal(String value)
        {
            this.value = value;
        }

        public String getValue()
        {
            return value;
        }

        @Override
        public String toString()
        {
            return value;
        }

        // You can add equals, hashCode, and other utility methods if required
    }

    public class ZeroOrMore
            implements Pattern
    {
        @Override
        public String toString()
        {
            return "%";
        }

        // Equals and hashCode methods might be required based on usage,
        // the record provided them by default
        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(); // No fields to hash in this case
        }
    }

    class Any
            implements Pattern
    {
        private final int length;

        public Any(int length)
        {
            checkArgument(length > 0, "Length must be > 0");
            this.length = length;
        }

        public int getLength()
        {
            return length;
        }

        @Override
        public String toString()
        {
            return Strings.repeat("_", length);
        }

        // You can add equals, hashCode, and other utility methods if required
    }
}
