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
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class ScalarHeader
{
    private final Optional<String> description;
    private final SqlFunctionVisibility visibility;
    private final boolean deterministic;
    private final boolean calledOnNullInput;
    private final Map<Signature, ScalarStatsHeader> signatureToScalarStatsHeaders;

    public ScalarHeader(Optional<String> description, SqlFunctionVisibility visibility, boolean deterministic, boolean calledOnNullInput)
    {
        this.description = description;
        this.visibility = visibility;
        this.deterministic = deterministic;
        this.calledOnNullInput = calledOnNullInput;
        this.signatureToScalarStatsHeaders = ImmutableMap.of();
    }

    public ScalarHeader(Optional<String> description, SqlFunctionVisibility visibility, boolean deterministic, boolean calledOnNullInput,
            Map<Signature, ScalarStatsHeader> signatureToScalarStatsHeaders)
    {
        this.description = description;
        this.visibility = visibility;
        this.deterministic = deterministic;
        this.calledOnNullInput = calledOnNullInput;
        this.signatureToScalarStatsHeaders = signatureToScalarStatsHeaders;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("description:", this.description)
                .add("visibility", this.visibility)
                .add("deterministic", this.deterministic)
                .add("calledOnNullInput", this.calledOnNullInput)
                .add("signatureToScalarStatsHeadersMap", Joiner.on(" , ").withKeyValueSeparator(" -> ").join(this.signatureToScalarStatsHeaders))
                .toString();
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

    public Map<Signature, ScalarStatsHeader> getSignatureToScalarStatsHeadersMap()
    {
        return signatureToScalarStatsHeaders;
    }
}
