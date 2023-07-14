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
package com.facebook.presto.spark;

import com.facebook.presto.Session;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.testing.TestingSession;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.spark.PrestoSparkQueryExecutionFactory.transferSessionPropertiesToSession;
import static org.testng.Assert.assertEquals;

public class TestPrestoSparkQueryExecutionFactory
{
    @Test
    public void testTransferSessionProperties()
    {
        Session.SessionBuilder sessionBuilder = TestingSession.testSessionBuilder();
        Map<String, String> sessionProperties = ImmutableMap.of(
                "property_name", "property_value",
                "catalog.property_name", "value2");

        Session result = transferSessionPropertiesToSession(sessionBuilder, sessionProperties).build();

        assertEquals(result.getSystemProperties().get("property_name"), "property_value");
        assertEquals(result.getUnprocessedCatalogProperties().get("catalog").get("property_name"), "value2");
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testTransferSessionPropertiesWithInvalidProperty()
    {
        Session.SessionBuilder sessionBuilder = TestingSession.testSessionBuilder();
        Map<String, String> sessionProperties = ImmutableMap.of(
                "invalid.property.name", "property_value");

        transferSessionPropertiesToSession(sessionBuilder, sessionProperties).build();
    }
}
