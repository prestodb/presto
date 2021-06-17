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
package com.facebook.presto.common.function;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class InvocationConvention
{
    private final List<InvocationArgumentConvention> argumentConventionList;
    private final InvocationReturnConvention returnConvention;
    private final boolean supportsSession;
    private final boolean supportsInstanceFactory;

    public InvocationConvention(
            List<InvocationArgumentConvention> argumentConventionList,
            InvocationReturnConvention returnConvention,
            boolean supportsSession,
            boolean supportsInstanceFactory)
    {
        this.argumentConventionList = Collections.unmodifiableList(requireNonNull(argumentConventionList, "argumentConventionList is null"));
        this.returnConvention = returnConvention;
        this.supportsSession = supportsSession;
        this.supportsInstanceFactory = supportsInstanceFactory;
    }

    public static InvocationConvention simpleConvention(InvocationReturnConvention returnConvention, InvocationArgumentConvention... argumentConventions)
    {
        return new InvocationConvention(Arrays.asList(argumentConventions), returnConvention, false, false);
    }

    public InvocationReturnConvention getReturnConvention()
    {
        return returnConvention;
    }

    public List<InvocationArgumentConvention> getArgumentConventions()
    {
        return argumentConventionList;
    }

    public InvocationArgumentConvention getArgumentConvention(int index)
    {
        return argumentConventionList.get(index);
    }

    public boolean supportsSession()
    {
        return supportsSession;
    }

    public boolean supportsInstanceFactor()
    {
        return supportsInstanceFactory;
    }

    public String toString()
    {
        return "(" + argumentConventionList.toString() + ")" + returnConvention;
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
        InvocationConvention that = (InvocationConvention) o;
        return supportsSession == that.supportsSession &&
                supportsInstanceFactory == that.supportsInstanceFactory &&
                Objects.equals(argumentConventionList, that.argumentConventionList) &&
                returnConvention == that.returnConvention;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(argumentConventionList, returnConvention, supportsSession, supportsInstanceFactory);
    }

    public enum InvocationArgumentConvention
    {
        /**
         * Argument must not be a boxed type. Argument will never be null.
         */
        NEVER_NULL(false, 1),
        /**
         * Argument is always an object type. A SQL null will be passed a Java null.
         */
        BOXED_NULLABLE(true, 1),
        /**
         * Argument must not be a boxed type, and is always followed with a boolean argument
         * to indicate if the sql value is null.
         */
        NULL_FLAG(true, 2),
        /**
         * Argument is passed a Block followed by the integer position in the block.  The
         * sql value may be null.
         */
        BLOCK_POSITION(true, 2),
        /**
         * Argument is a lambda function.
         */
        FUNCTION(false, 1);

        private final boolean nullable;
        private final int parameterCount;

        InvocationArgumentConvention(boolean nullable, int parameterCount)
        {
            this.nullable = nullable;
            this.parameterCount = parameterCount;
        }

        public boolean isNullable()
        {
            return nullable;
        }

        public int getParameterCount()
        {
            return parameterCount;
        }
    }

    public enum InvocationReturnConvention
    {
        FAIL_ON_NULL(false),
        NULLABLE_RETURN(true);

        private final boolean nullable;

        InvocationReturnConvention(boolean nullable)
        {
            this.nullable = nullable;
        }

        public boolean isNullable()
        {
            return nullable;
        }
    }
}
