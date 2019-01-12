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
package io.prestosql.connector.informationSchema;

import com.google.common.collect.ImmutableList;
import io.prestosql.metadata.InternalNodeManager;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class InformationSchemaSplitManager
        implements ConnectorSplitManager
{
    private final InternalNodeManager nodeManager;

    public InformationSchemaSplitManager(InternalNodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        InformationSchemaTableLayoutHandle handle = (InformationSchemaTableLayoutHandle) layout;

        List<HostAddress> localAddress = ImmutableList.of(nodeManager.getCurrentNode().getHostAndPort());

        ConnectorSplit split = new InformationSchemaSplit(handle.getTable(), handle.getPrefixes(), localAddress);

        return new FixedSplitSource(ImmutableList.of(split));
    }
}
