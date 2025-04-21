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
package com.facebook.presto.tpcds;

import com.facebook.presto.common.experimental.FbThriftUtils;
import com.facebook.presto.common.experimental.ThriftSerializationRegistry;
import com.facebook.presto.common.experimental.auto_gen.ThriftConnectorTransactionHandle;
import com.facebook.presto.common.experimental.auto_gen.ThriftTpcdsTransactionHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

public enum TpcdsTransactionHandle
        implements ConnectorTransactionHandle
{
    INSTANCE;

    static {
        ThriftSerializationRegistry.registerSerializer(TpcdsTransactionHandle.class, TpcdsTransactionHandle::toThriftInterface, null);
        ThriftSerializationRegistry.registerDeserializer(TpcdsTransactionHandle.class, ThriftTpcdsTransactionHandle.class, TpcdsTransactionHandle::deserialize, TpcdsTransactionHandle::createTpcdsTransactionHandle);
    }

    public static TpcdsTransactionHandle createTpcdsTransactionHandle(ThriftTpcdsTransactionHandle thriftHandle)
    {
        return TpcdsTransactionHandle.valueOf(thriftHandle.getValue());
    }

    @Override
    public ThriftTpcdsTransactionHandle toThrift()
    {
        return new ThriftTpcdsTransactionHandle(this.name());
    }

    @Override
    public ThriftConnectorTransactionHandle toThriftInterface()
    {
        return ThriftConnectorTransactionHandle.builder()
                .setType(getImplementationType())
                .setSerializedConnectorTransactionHandle(FbThriftUtils.serialize(this.toThrift()))
                .build();
    }

    public static TpcdsTransactionHandle deserialize(byte[] bytes)
    {
        return TpcdsTransactionHandle.valueOf(FbThriftUtils.deserialize(ThriftTpcdsTransactionHandle.class, bytes).getValue());
    }
}
