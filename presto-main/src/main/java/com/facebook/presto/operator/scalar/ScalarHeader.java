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

import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.facebook.presto.spi.function.scalar.ScalarFunctionStatsHeader;
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
    private final Map<Signature, ScalarFunctionStatsHeader> signatureToScalarFunctionStatsHeaders;

    public ScalarHeader(Optional<String> description, SqlFunctionVisibility visibility, boolean deterministic, boolean calledOnNullInput)
    {
        this.description = description;
        this.visibility = visibility;
        this.deterministic = deterministic;
        this.calledOnNullInput = calledOnNullInput;
        this.signatureToScalarFunctionStatsHeaders = ImmutableMap.of();
    }

    public ScalarHeader(Optional<String> description, SqlFunctionVisibility visibility, boolean deterministic, boolean calledOnNullInput,
            Map<Signature, ScalarFunctionStatsHeader> signatureToScalarFunctionStatsHeaders)
    {
        this.description = description;
        this.visibility = visibility;
        this.deterministic = deterministic;
        this.calledOnNullInput = calledOnNullInput;
        this.signatureToScalarFunctionStatsHeaders = signatureToScalarFunctionStatsHeaders;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("description:", this.description)
                .add("visibility", this.visibility)
                .add("deterministic", this.deterministic)
                .add("calledOnNullInput", this.calledOnNullInput)
                .add("signatureToScalarFunctionStatsHeadersMap", Joiner.on(" , ").withKeyValueSeparator(" -> ").join(this.signatureToScalarFunctionStatsHeaders))
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

    public Map<Signature, ScalarFunctionStatsHeader> getSignatureToScalarFunctionStatsHeadersMap()
    {
        return signatureToScalarFunctionStatsHeaders;
    }
}
