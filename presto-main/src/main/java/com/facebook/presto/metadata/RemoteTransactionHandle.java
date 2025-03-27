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
package com.facebook.presto.metadata;

import com.facebook.presto.common.experimental.ThriftSerializationRegistry;
import com.facebook.presto.common.experimental.auto_gen.ThriftConnectorTransactionHandle;
import com.facebook.presto.common.experimental.auto_gen.ThriftRemoteTransactionHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

public class RemoteTransactionHandle
        implements ConnectorTransactionHandle
{
    static {
//        ThriftSerializationRegistry.registerSerializer(RemoteTransactionHandle.class, RemoteTransactionHandle::serialize);
        ThriftSerializationRegistry.registerDeserializer("REMOTE_TRANSACTION_HANDLE", RemoteTransactionHandle::deserialize);
    }

    @JsonCreator
    public RemoteTransactionHandle()
    {
    }

    @JsonProperty
    public String getDummyValue()
    {
        // Necessary for Jackson serialization
        return null;
    }

    public RemoteTransactionHandle(ThriftRemoteTransactionHandle thriftHandle)
    {
        this();
    }

    @Override
    public String getImplementationType()
    {
        return "REMOTE_TRANSACTION_HANDLE";
    }

    @Override
    public ThriftConnectorTransactionHandle toThriftInterface()
    {
        try {
            TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
            ThriftConnectorTransactionHandle thriftHandle = new ThriftConnectorTransactionHandle();
            thriftHandle.setType(getImplementationType());
            thriftHandle.setSerializedConnectorTransactionHandle(serializer.serialize(new ThriftRemoteTransactionHandle()));
            return thriftHandle;
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    public static RemoteTransactionHandle deserialize(byte[] bytes)
    {
        try {
            ThriftRemoteTransactionHandle thriftHandle = new ThriftRemoteTransactionHandle();
            TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
            deserializer.deserialize(thriftHandle, bytes);
            return new RemoteTransactionHandle(thriftHandle);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }
}
