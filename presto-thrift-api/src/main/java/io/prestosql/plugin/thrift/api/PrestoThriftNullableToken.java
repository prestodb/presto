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
package io.prestosql.plugin.thrift.api;

import io.airlift.drift.annotations.ThriftConstructor;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;

import javax.annotation.Nullable;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.drift.annotations.ThriftField.Requiredness.OPTIONAL;

@ThriftStruct
public final class PrestoThriftNullableToken
{
    private final PrestoThriftId token;

    @ThriftConstructor
    public PrestoThriftNullableToken(@Nullable PrestoThriftId token)
    {
        this.token = token;
    }

    @Nullable
    @ThriftField(value = 1, requiredness = OPTIONAL)
    public PrestoThriftId getToken()
    {
        return token;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PrestoThriftNullableToken other = (PrestoThriftNullableToken) obj;
        return Objects.equals(this.token, other.token);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(token);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("token", token)
                .toString();
    }
}
