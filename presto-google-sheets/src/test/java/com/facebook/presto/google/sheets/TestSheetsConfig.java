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
package com.facebook.presto.google.sheets;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestSheetsConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SheetsConfig.class)
                .setCredentialsFilePath(null)
                .setMetadataSheetId(null)
                .setSheetsDataMaxCacheSize(1000)
                .setSheetsDataExpireAfterWrite(new Duration(5, TimeUnit.MINUTES)));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path credentialsFile = Files.createTempFile(null, null);

        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("credentials-path", credentialsFile.toString())
                .put("metadata-sheet-id", "foo_bar_sheet_id#Sheet1")
                .put("sheets-data-max-cache-size", "2000")
                .put("sheets-data-expire-after-write", "10m")
                .build();

        SheetsConfig expected = new SheetsConfig()
                .setCredentialsFilePath(credentialsFile.toString())
                .setMetadataSheetId("foo_bar_sheet_id#Sheet1")
                .setSheetsDataMaxCacheSize(2000)
                .setSheetsDataExpireAfterWrite(new Duration(10, TimeUnit.MINUTES));

        assertFullMapping(properties, expected);
    }
}
