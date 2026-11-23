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
package com.facebook.presto.sql.planner.optimizations;

import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;

public final class RpcArgumentLayout
{
    private final OptionalInt optionsArgumentIndex;

    private RpcArgumentLayout(OptionalInt optionsArgumentIndex)
    {
        this.optionsArgumentIndex = optionsArgumentIndex;
    }

    public static RpcArgumentLayout optionsAt(int index)
    {
        checkArgument(index >= 0, "options argument index must be non-negative");
        return new RpcArgumentLayout(OptionalInt.of(index));
    }

    public static RpcArgumentLayout noOptions()
    {
        return new RpcArgumentLayout(OptionalInt.empty());
    }

    public OptionalInt getOptionsArgumentIndex()
    {
        return optionsArgumentIndex;
    }
}
