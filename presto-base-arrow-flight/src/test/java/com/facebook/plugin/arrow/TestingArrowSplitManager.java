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
package com.facebook.plugin.arrow;

import com.facebook.presto.spi.NodeManager;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.flight.FlightDescriptor;

import javax.inject.Inject;

import java.util.Optional;

public class TestingArrowSplitManager
        extends AbstractArrowSplitManager
{
    private final TestingArrowFlightConfig testConfig;

    private final NodeManager nodeManager;

    private final ArrowFlightConfig config;

    @Inject
    public TestingArrowSplitManager(ArrowFlightConfig config, AbstractArrowFlightClientHandler client, TestingArrowFlightConfig testConfig, NodeManager nodeManager)
    {
        super(client, config);
        this.testConfig = testConfig;
        this.nodeManager = nodeManager;
        this.config = config;
    }

    @Override
    protected FlightDescriptor getFlightDescriptor(ArrowTableLayoutHandle tableLayoutHandle)
    {
        ArrowTableHandle tableHandle = tableLayoutHandle.getTableHandle();
        Optional<String> query = Optional.of(new TestingArrowQueryBuilder().buildSql(tableHandle.getSchema(),
                tableHandle.getTable(),
                tableLayoutHandle.getColumnHandles(), ImmutableMap.of(),
                tableLayoutHandle.getTupleDomain()));
        TestingArrowFlightRequest request = new TestingArrowFlightRequest(config, testConfig, tableHandle.getSchema(), tableHandle.getTable(), query, nodeManager.getWorkerNodes().size());
        return FlightDescriptor.command(request.getCommand());
    }
}
