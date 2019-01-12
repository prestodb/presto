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
package io.prestosql.plugin.mongodb;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;

import javax.inject.Inject;

import java.util.List;

import static io.prestosql.spi.HostAddress.fromParts;
import static java.util.stream.Collectors.toList;

public class MongoSplitManager
        implements ConnectorSplitManager
{
    private final List<HostAddress> addresses;

    @Inject
    public MongoSplitManager(MongoClientConfig config)
    {
        this.addresses = config.getSeeds().stream()
                .map(s -> fromParts(s.getHost(), s.getPort()))
                .collect(toList());
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        MongoTableLayoutHandle tableLayout = (MongoTableLayoutHandle) layout;
        MongoTableHandle tableHandle = tableLayout.getTable();

        MongoSplit split = new MongoSplit(
                tableHandle.getSchemaTableName(),
                tableLayout.getTupleDomain(),
                addresses);

        return new FixedSplitSource(ImmutableList.of(split));
    }
}
