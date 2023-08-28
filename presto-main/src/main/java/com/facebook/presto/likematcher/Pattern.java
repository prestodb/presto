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

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

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

    class Any
            implements Pattern
    {
        private final int min;
        private final boolean unbounded;

        public Any(int min, boolean unbounded)
        {
            checkArgument(min > 0 || unbounded, "Any must be unbounded or require at least 1 character");
            this.min = min;
            this.unbounded = unbounded;
        }

        public int getMin()
        {
            return min;
        }

        public boolean isUnbounded()
        {
            return unbounded;
        }

        @Override
        public String toString()
        {
            return format("{%s%s}", min, unbounded ? "+" : "");
        }

        // You can add equals, hashCode, and other utility methods if required
    }
}
