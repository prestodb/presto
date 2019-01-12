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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.BaseEncoding;
import io.airlift.drift.annotations.ThriftConstructor;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;

import java.util.Arrays;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class PrestoThriftId
{
    private static final int PREFIX_SUFFIX_BYTES = 8;
    private static final String FILLER = "..";
    private static final int MAX_DISPLAY_CHARACTERS = PREFIX_SUFFIX_BYTES * 4 + FILLER.length();

    private final byte[] id;

    @JsonCreator
    @ThriftConstructor
    public PrestoThriftId(@JsonProperty("id") byte[] id)
    {
        this.id = requireNonNull(id, "id is null");
    }

    @JsonProperty
    @ThriftField(1)
    public byte[] getId()
    {
        return id;
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
        PrestoThriftId other = (PrestoThriftId) obj;
        return Arrays.equals(this.id, other.id);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(id);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", summarize(id))
                .toString();
    }

    @VisibleForTesting
    static String summarize(byte[] value)
    {
        if (value.length * 2 <= MAX_DISPLAY_CHARACTERS) {
            return BaseEncoding.base16().encode(value);
        }
        return BaseEncoding.base16().encode(value, 0, PREFIX_SUFFIX_BYTES)
                + FILLER
                + BaseEncoding.base16().encode(value, value.length - PREFIX_SUFFIX_BYTES, PREFIX_SUFFIX_BYTES);
    }
}
