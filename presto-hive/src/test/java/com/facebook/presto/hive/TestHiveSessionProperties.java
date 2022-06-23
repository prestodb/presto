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
package com.facebook.presto.hive;

import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.facebook.presto.testing.TestingConnectorSession;
import org.testng.annotations.Test;

import static com.facebook.presto.hive.HiveSessionProperties.getNodeSelectionStrategy;
import static com.facebook.presto.hive.HiveSessionProperties.isCacheEnabled;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.HARD_AFFINITY;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHiveSessionProperties
{
    @Test
    public void testEmptyNodeSelectionStrategyConfig()
    {
        ConnectorSession connectorSession = new TestingConnectorSession(
                new HiveSessionProperties(
                        new HiveClientConfig(),
                        new OrcFileWriterConfig(),
                        new ParquetFileWriterConfig(),
                        new CacheConfig()).getSessionProperties());
        assertEquals(getNodeSelectionStrategy(connectorSession), NO_PREFERENCE);
    }

    @Test
    public void testEmptyConfigNodeSelectionStrategyConfig()
    {
        ConnectorSession connectorSession = new TestingConnectorSession(
                new HiveSessionProperties(
                        new HiveClientConfig().setNodeSelectionStrategy(NodeSelectionStrategy.valueOf("NO_PREFERENCE")),
                        new OrcFileWriterConfig(),
                        new ParquetFileWriterConfig(),
                        new CacheConfig()).getSessionProperties());
        assertEquals(getNodeSelectionStrategy(connectorSession), NO_PREFERENCE);
    }

    @Test
    public void testNodeSelectionStrategyConfig()
    {
        ConnectorSession connectorSession = new TestingConnectorSession(
                new HiveSessionProperties(
                        new HiveClientConfig().setNodeSelectionStrategy(HARD_AFFINITY),
                        new OrcFileWriterConfig(),
                        new ParquetFileWriterConfig(),
                        new CacheConfig()).getSessionProperties());
        assertEquals(getNodeSelectionStrategy(connectorSession), HARD_AFFINITY);
    }

    @Test
    public void testCacheEnabledConfig()
    {
        ConnectorSession connectorSession = new TestingConnectorSession(
                new HiveSessionProperties(
                        new HiveClientConfig(),
                        new OrcFileWriterConfig(),
                        new ParquetFileWriterConfig(),
                        new CacheConfig().setCachingEnabled(true)).getSessionProperties());
        assertTrue(isCacheEnabled(connectorSession));
    }
}
