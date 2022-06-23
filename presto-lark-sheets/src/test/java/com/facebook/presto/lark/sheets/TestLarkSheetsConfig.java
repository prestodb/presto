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
package com.facebook.presto.lark.sheets;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static com.facebook.presto.lark.sheets.LarkSheetsConfig.Domain.FEISHU;
import static com.facebook.presto.lark.sheets.LarkSheetsConfig.Domain.LARK;

public class TestLarkSheetsConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(LarkSheetsConfig.class)
                .setAppDomain(LARK)
                .setAppId(null)
                .setAppSecretFile(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("app-domain", "FEISHU")
                .put("app-id", "app-id")
                .put("app-secret-file", "app-secret.json")
                .build();

        LarkSheetsConfig expected = new LarkSheetsConfig()
                .setAppDomain(FEISHU)
                .setAppId("app-id")
                .setAppSecretFile("app-secret.json");

        assertFullMapping(properties, expected);
    }
}
