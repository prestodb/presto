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

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.TupleDomain;

public final class Util
{
    private Util()
    {
    }

    public static TupleDomain<ConnectorColumnHandle> toConnectorDomain(TupleDomain<ColumnHandle> domain)
    {
        return domain.transform(new TupleDomain.Function<ColumnHandle, ConnectorColumnHandle>()
        {
            @Override
            public ConnectorColumnHandle apply(ColumnHandle input)
            {
                return input.getConnectorHandle();
            }
        });
    }

    public static TupleDomain<ColumnHandle> fromConnectorDomain(final String connectorId, TupleDomain<ConnectorColumnHandle> domain)
    {
        return domain.transform(new TupleDomain.Function<ConnectorColumnHandle, ColumnHandle>()
        {
            @Override
            public ColumnHandle apply(ConnectorColumnHandle input)
            {
                return new ColumnHandle(connectorId, input);
            }
        });
    }
}
