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
package com.facebook.presto.tpch;

import com.facebook.presto.common.experimental.FbThriftUtils;
import com.facebook.presto.common.experimental.ThriftSerializationRegistry;
import com.facebook.presto.common.experimental.TypeAdapter;
import com.facebook.presto.common.experimental.auto_gen.ThriftColumnHandle;
import com.facebook.presto.common.experimental.auto_gen.ThriftTpchColumnHandle;
import com.facebook.presto.common.experimental.auto_gen.ThriftType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class TpchColumnHandle
        implements ColumnHandle
{
    private final String columnName;
    private final Type type;

    static {
        ThriftSerializationRegistry.registerSerializer(TpchColumnHandle.class, TpchColumnHandle::toThrift, null);
        ThriftSerializationRegistry.registerDeserializer(TpchColumnHandle.class, ThriftTpchColumnHandle.class, TpchColumnHandle::deserialize, null);
    }

    public TpchColumnHandle(ThriftTpchColumnHandle thriftHandle)
    {
        this(thriftHandle.getColumnName(), (Type) TypeAdapter.fromThrift(thriftHandle.getType()));
    }

    @JsonCreator
    public TpchColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("type") Type type)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.type = requireNonNull(type, "type is null");
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return "tpch:" + columnName;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if ((o == null) || (getClass() != o.getClass())) {
            return false;
        }
        TpchColumnHandle other = (TpchColumnHandle) o;
        return Objects.equals(columnName, other.columnName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName);
    }

    @Override
    public ThriftColumnHandle toThriftInterface()
    {
        return ThriftColumnHandle.builder()
                .setType(getImplementationType())
                .setSerializedHandle(FbThriftUtils.serialize(this.toThrift()))
                .build();
    }

    @Override
    public ThriftTpchColumnHandle toThrift()
    {
        return new ThriftTpchColumnHandle(columnName, (ThriftType) type.toThriftInterface());
    }

    public static TpchColumnHandle deserialize(byte[] bytes)
    {
        return new TpchColumnHandle(FbThriftUtils.deserialize(ThriftTpchColumnHandle.class, bytes));
    }

    public static byte[] serialize(TpchColumnHandle tpchColumnHandle)
    {
        return FbThriftUtils.serialize(tpchColumnHandle.toThrift());
    }
}
