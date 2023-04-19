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
import com.facebook.presto.orc.OrcWriterOptions;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Properties;

import static com.facebook.presto.hive.HiveTestUtils.HIVE_CLIENT_CONFIG;
import static com.facebook.presto.hive.HiveTestUtils.METASTORE_CLIENT_CONFIG;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultOrcFileWriterFactory;
import static com.facebook.presto.hive.OrcFileWriterFactory.ORC_FLAT_MAP_COLUMN_NUMBERS_KEY;
import static com.facebook.presto.hive.OrcFileWriterFactory.ORC_FLAT_MAP_KEY_LIMIT_KEY;
import static com.facebook.presto.hive.OrcFileWriterFactory.ORC_FLAT_MAP_WRITER_ENABLED_KEY;
import static com.facebook.presto.hive.OrcFileWriterFactory.ORC_MAP_STATISTICS_KEY;
import static com.facebook.presto.orc.OrcWriterOptions.DEFAULT_MAX_FLATTENED_MAP_KEY_COUNT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TesOrcFileWriterFactory
{
    @Test
    public void tesDefaultFlatMapOptions()
    {
        Properties serDe = new Properties();
        OrcWriterOptions orcWriterOptions = getOrcWriterOptions(serDe);
        assertEquals(orcWriterOptions.getMaxFlattenedMapKeyCount(), DEFAULT_MAX_FLATTENED_MAP_KEY_COUNT);
        assertFalse(orcWriterOptions.isMapStatisticsEnabled());
        assertEquals(orcWriterOptions.getFlattenedColumns().size(), 0);
    }

    @Test
    public void testFlatMapColumnsDisabled()
    {
        Properties serDe = new Properties();
        serDe.setProperty(ORC_FLAT_MAP_WRITER_ENABLED_KEY, "false");
        serDe.setProperty(ORC_FLAT_MAP_COLUMN_NUMBERS_KEY, "1,2");

        OrcWriterOptions orcWriterOptions = getOrcWriterOptions(serDe);
        assertEquals(orcWriterOptions.getFlattenedColumns().size(), 0);
    }

    @Test
    public void testFlatMapColumnsEnabled()
    {
        Properties serDe = new Properties();
        serDe.setProperty(ORC_FLAT_MAP_WRITER_ENABLED_KEY, "true");
        serDe.setProperty(ORC_FLAT_MAP_COLUMN_NUMBERS_KEY, "1,2");

        OrcWriterOptions orcWriterOptions = getOrcWriterOptions(serDe);
        assertEquals(orcWriterOptions.getFlattenedColumns(), ImmutableSet.of(1, 2));
    }

    @Test
    public void testFlatMapKeyLimit()
    {
        Properties serDe = new Properties();
        serDe.setProperty(ORC_FLAT_MAP_KEY_LIMIT_KEY, "23");

        OrcWriterOptions orcWriterOptions = getOrcWriterOptions(serDe);
        assertEquals(orcWriterOptions.getMaxFlattenedMapKeyCount(), 23);
    }

    @Test
    public void testFlatMapStatsEnabled()
    {
        Properties serDe = new Properties();
        serDe.setProperty(ORC_MAP_STATISTICS_KEY, "true");

        OrcWriterOptions orcWriterOptions = getOrcWriterOptions(serDe);
        assertTrue(orcWriterOptions.isMapStatisticsEnabled());
    }

    private static OrcWriterOptions getOrcWriterOptions(Properties serDe)
    {
        OrcFileWriterConfig orcFileWriterConfig = new OrcFileWriterConfig();
        orcFileWriterConfig.setFlatMapWriterEnabled(true);
        OrcFileWriterFactory orcFileWriterFactory = getDefaultOrcFileWriterFactory(HIVE_CLIENT_CONFIG, METASTORE_CLIENT_CONFIG);
        HiveSessionProperties sessionProperties = new HiveSessionProperties(
                new HiveClientConfig(),
                orcFileWriterConfig,
                new ParquetFileWriterConfig(),
                new CacheConfig());
        ConnectorSession session = new TestingConnectorSession(sessionProperties.getSessionProperties());
        return orcFileWriterFactory.buildOrcWriterOptions(session, serDe);
    }
}
