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
package com.facebook.presto.functionNamespace;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static com.facebook.presto.spi.function.FunctionImplementationType.SQL;
import static com.facebook.presto.spi.function.FunctionImplementationType.THRIFT;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Language;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.testng.Assert.assertEquals;

public class TestSqlInvokedFunctionNamespaceManagerConfig
{
    @Test
    public void testDefault()
    {
        assertRecordedDefaults(recordDefaults(SqlInvokedFunctionNamespaceManagerConfig.class)
                .setFunctionCacheExpiration(new Duration(5, MINUTES))
                .setFunctionInstanceCacheExpiration(new Duration(8, HOURS))
                .setSupportedFunctionLanguages("{\"sql\": \"SQL\"}"));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("function-cache-expiration", "10m")
                .put("function-instance-cache-expiration", "4h")
                .put("supported-function-languages", "{\"sql\": \"SQL\", \"hive\": \"THRIFT\"}")
                .build();
        SqlInvokedFunctionNamespaceManagerConfig expected = new SqlInvokedFunctionNamespaceManagerConfig()
                .setFunctionCacheExpiration(new Duration(10, MINUTES))
                .setFunctionInstanceCacheExpiration(new Duration(4, HOURS))
                .setSupportedFunctionLanguages("{\"sql\": \"SQL\", \"hive\": \"THRIFT\"}");

        assertFullMapping(properties, expected);
        assertEquals(
                expected.getSupportedFunctionLanguages(),
                ImmutableMap.of(new Language("sql"), SQL, new Language("hive"), THRIFT));
    }
}
