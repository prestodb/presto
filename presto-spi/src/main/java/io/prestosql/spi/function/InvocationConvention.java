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
package io.prestosql.spi.function;

import java.util.List;

public class InvocationConvention
{
    private final List<InvocationArgumentConvention> argumentConventionList;
    private final InvocationReturnConvention returnConvention;
    private final boolean hasSession;

    public InvocationConvention(List<InvocationArgumentConvention> argumentConventionList, InvocationReturnConvention returnConvention, boolean hasSession)
    {
        this.argumentConventionList = argumentConventionList;
        this.returnConvention = returnConvention;
        this.hasSession = hasSession;
    }

    public InvocationReturnConvention getReturnConvention()
    {
        return returnConvention;
    }

    public InvocationArgumentConvention getArgumentConvention(int index)
    {
        return argumentConventionList.get(index);
    }

    public boolean hasSession()
    {
        return hasSession;
    }

    public String toString()
    {
        return "(" + argumentConventionList.toString() + ")" + returnConvention;
    }

    public enum InvocationArgumentConvention
    {
        NEVER_NULL,
        BOXED_NULLABLE,
        NULL_FLAG,
        BLOCK_POSITION,
        FUNCTION
    }

    public enum InvocationReturnConvention
    {
        FAIL_ON_NULL,
        NULLABLE_RETURN
    }
}
