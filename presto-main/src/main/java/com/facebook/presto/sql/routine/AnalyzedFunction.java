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
package com.facebook.presto.sql.routine;

import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class AnalyzedFunction
{
    private final String name;
    private final Type returnType;
    private final List<Type> argumentTypes;
    private final SqlRoutine routine;

    public AnalyzedFunction(String name, Type returnType, List<Type> argumentTypes, SqlRoutine routine)
    {
        this.name = requireNonNull(name, "name is null");
        this.returnType = requireNonNull(returnType, "returnType is null");
        this.argumentTypes = ImmutableList.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
        this.routine = requireNonNull(routine, "routine is null");
    }

    public String getName()
    {
        return name;
    }

    public Type getReturnType()
    {
        return returnType;
    }

    public List<Type> getArgumentTypes()
    {
        return argumentTypes;
    }

    public SqlRoutine getRoutine()
    {
        return routine;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("returnType", returnType)
                .add("argumentTypes", argumentTypes)
                .add("routine", routine)
                .toString();
    }
}
