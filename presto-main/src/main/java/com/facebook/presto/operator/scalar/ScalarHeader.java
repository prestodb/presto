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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.function.ScalarStatsHeader;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;

public class ScalarHeader
{
    private final Optional<String> description;
    private final SqlFunctionVisibility visibility;
    private final boolean deterministic;
    private final boolean calledOnNullInput;
    private final Map<Signature, ScalarStatsHeader> scalarStatsHeader;

    public ScalarHeader(Optional<String> description, SqlFunctionVisibility visibility, boolean deterministic, boolean calledOnNullInput)
    {
        this.description = description;
        this.visibility = visibility;
        this.deterministic = deterministic;
        this.calledOnNullInput = calledOnNullInput;
        this.scalarStatsHeader = ImmutableMap.of();
    }

    public ScalarHeader(Optional<String> description, SqlFunctionVisibility visibility, boolean deterministic, boolean calledOnNullInput,
            Map<Signature, ScalarStatsHeader> scalarStatsHeader)
    {
        this.description = description;
        this.visibility = visibility;
        this.deterministic = deterministic;
        this.calledOnNullInput = calledOnNullInput;
        this.scalarStatsHeader = scalarStatsHeader;
    }

    @Override
    public String toString()
    {
        return String.format("ScalarHeader: description: %s visibility:%s deterministic: %b calledOnNullInput %b scalarStatsHeader %s",
                description, visibility, deterministic, calledOnNullInput, scalarStatsHeader);
    }

    public Optional<String> getDescription()
    {
        return description;
    }

    public SqlFunctionVisibility getVisibility()
    {
        return visibility;
    }

    public boolean isDeterministic()
    {
        return deterministic;
    }

    public boolean isCalledOnNullInput()
    {
        return calledOnNullInput;
    }

    public Map<Signature, ScalarStatsHeader> getScalarStatsHeader()
    {
        return scalarStatsHeader;
    }
}
