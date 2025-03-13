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
package com.facebook.presto.common.type;

import com.facebook.presto.common.experimental.ThriftSerializationRegistry;
import com.facebook.presto.common.experimental.auto_gen.ThriftType;
import com.facebook.presto.common.experimental.auto_gen.ThriftVarcharType;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TJSONProtocol;

import static java.util.Collections.singletonList;

public final class VarcharType
        extends AbstractVarcharType
{
    static {
        ThriftSerializationRegistry.registerSerializer(VarcharType.class, VarcharType::toThrift, null);
        ThriftSerializationRegistry.registerDeserializer(VarcharType.class, ThriftVarcharType.class, VarcharType::deserialize, null);
    }

    public VarcharType(ThriftVarcharType thriftVarcharType)
    {
        this(thriftVarcharType.getLength());
    }

    public static final VarcharType VARCHAR = new VarcharType(UNBOUNDED_LENGTH);

    public static VarcharType createUnboundedVarcharType()
    {
        return VARCHAR;
    }

    public static VarcharType createVarcharType(int length)
    {
        if (length > MAX_LENGTH || length < 0) {
            // Use createUnboundedVarcharType for unbounded VARCHAR.
            throw new IllegalArgumentException("Invalid VARCHAR length " + length);
        }
        return new VarcharType(length);
    }

    public static TypeSignature getParametrizedVarcharSignature(String param)
    {
        return new TypeSignature(StandardTypes.VARCHAR, TypeSignatureParameter.of(param));
    }

    private VarcharType(int length)
    {
        super(
                length,
                new TypeSignature(
                        StandardTypes.VARCHAR,
                        singletonList(TypeSignatureParameter.of((long) length))));
    }

    @Override
    public ThriftType toThriftInterface()
    {
        try {
            TSerializer serializer = new TSerializer(new TJSONProtocol.Factory());
            ThriftType thriftType = new ThriftType();
            thriftType.setType(getImplementationType());
            thriftType.setSerializedData(serializer.serialize(this.toThrift()));
            return thriftType;
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ThriftVarcharType toThrift()
    {
        return new ThriftVarcharType(getLengthSafe());
    }

    public static VarcharType deserialize(byte[] bytes)
    {
        try {
            TDeserializer deserializer = new TDeserializer(new TJSONProtocol.Factory());
            ThriftVarcharType thriftType = new ThriftVarcharType();
            deserializer.deserialize(thriftType, bytes);
            return new VarcharType(thriftType);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }
}
