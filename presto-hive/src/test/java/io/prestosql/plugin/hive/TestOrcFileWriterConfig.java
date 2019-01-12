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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class TestOrcFileWriterConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(OrcFileWriterConfig.class)
                .setStripeMinSize(new DataSize(32, MEGABYTE))
                .setStripeMaxSize(new DataSize(64, MEGABYTE))
                .setStripeMaxRowCount(10_000_000)
                .setRowGroupMaxRowCount(10_000)
                .setDictionaryMaxMemory(new DataSize(16, MEGABYTE))
                .setStringStatisticsLimit(new DataSize(64, BYTE))
                .setMaxCompressionBufferSize(new DataSize(256, KILOBYTE)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.orc.writer.stripe-min-size", "13MB")
                .put("hive.orc.writer.stripe-max-size", "27MB")
                .put("hive.orc.writer.stripe-max-rows", "44")
                .put("hive.orc.writer.row-group-max-rows", "11")
                .put("hive.orc.writer.dictionary-max-memory", "13MB")
                .put("hive.orc.writer.string-statistics-limit", "17MB")
                .put("hive.orc.writer.max-compression-buffer-size", "19MB")
                .build();

        OrcFileWriterConfig expected = new OrcFileWriterConfig()
                .setStripeMinSize(new DataSize(13, MEGABYTE))
                .setStripeMaxSize(new DataSize(27, MEGABYTE))
                .setStripeMaxRowCount(44)
                .setRowGroupMaxRowCount(11)
                .setDictionaryMaxMemory(new DataSize(13, MEGABYTE))
                .setStringStatisticsLimit(new DataSize(17, MEGABYTE))
                .setMaxCompressionBufferSize(new DataSize(19, MEGABYTE));

        assertFullMapping(properties, expected);
    }
}
