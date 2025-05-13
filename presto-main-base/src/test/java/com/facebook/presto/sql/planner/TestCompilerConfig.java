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
package com.facebook.presto.sql.planner;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestCompilerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(CompilerConfig.class)
                .setExpressionCacheSize(10_000)
                .setLeafNodeLimitEnabled(false)
                .setLeafNodeLimit(10_000));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("compiler.expression-cache-size", "52")
                .put("planner.max-leaf-nodes-in-plan", "100")
                .put("planner.leaf-node-limit-enabled", "true")
                .build();

        CompilerConfig expected = new CompilerConfig()
                .setExpressionCacheSize(52)
                .setLeafNodeLimit(100)
                .setLeafNodeLimitEnabled(true);

        assertFullMapping(properties, expected);
    }
}
