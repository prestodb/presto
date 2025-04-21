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
import com.facebook.presto.common.experimental.auto_gen.ThriftConnectorTransactionHandle;
import com.facebook.presto.common.experimental.auto_gen.ThriftTpchTransactionHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

public enum TpchTransactionHandle
        implements ConnectorTransactionHandle
{
    INSTANCE;

    static {
        ThriftSerializationRegistry.registerSerializer(TpchTransactionHandle.class, TpchTransactionHandle::toThriftInterface, null);
        ThriftSerializationRegistry.registerDeserializer(TpchTransactionHandle.class, ThriftTpchTransactionHandle.class, TpchTransactionHandle::deserialize, TpchTransactionHandle::createTpchTransactionHandle);
    }

    public static TpchTransactionHandle createTpchTransactionHandle(ThriftTpchTransactionHandle thriftHandle)
    {
        return TpchTransactionHandle.valueOf(thriftHandle.getValue());
    }

    @Override
    public ThriftConnectorTransactionHandle toThriftInterface()
    {
        return ThriftConnectorTransactionHandle.builder()
                .setType(getImplementationType())
                .setSerializedConnectorTransactionHandle(FbThriftUtils.serialize(this.toThrift()))
                .build();
    }

    @Override
    public ThriftTpchTransactionHandle toThrift()
    {
        return new ThriftTpchTransactionHandle(this.name());
    }

    public static TpchTransactionHandle deserialize(byte[] bytes)
    {
        return TpchTransactionHandle.valueOf(FbThriftUtils.deserialize(ThriftTpchTransactionHandle.class, bytes).getValue());
    }
}
