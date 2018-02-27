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
package com.facebook.presto.connector.thrift;

import com.facebook.presto.connector.thrift.api.PrestoThriftBlock;
import com.facebook.presto.connector.thrift.api.PrestoThriftSession;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.doubleSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.longSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringSessionProperty;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestThriftSessionProperties
{
    private final List<PropertyMetadata<?>> propertyMetadataList = ImmutableList.of(
            booleanSessionProperty("test_bool_prop", "boolean session property", true, false),
            booleanSessionProperty("test_bool_prop_nullable", "nullable boolean session property", null, false),
            integerSessionProperty("test_int_prop", "integer session property", 100, false),
            integerSessionProperty("test_int_prop_nullable", "nullable integer session property", null, false),
            longSessionProperty("test_long_prop", "long session property", 1000L, false),
            longSessionProperty("test_long_prop_nullable", "nullable long session property", null, false),
            doubleSessionProperty("test_double_prop", "double session property", 1.9, false),
            doubleSessionProperty("test_double_prop_nullable", "nullable double session property", null, false),
            stringSessionProperty("test_string_prop", "string session property", "This is a string", false),
            stringSessionProperty("test_string_prop_nullable", "nullable string session property", null, false));

    private ThriftSessionProperties sessionProperties = new ThriftSessionProperties(
            new TestingThriftServiceProvider(
                    new TestingThriftService(propertyMetadataList)));

    @Test
    public void testGetSessionProperties()
    {
        List<PropertyMetadata<?>> registeredProperty = sessionProperties.getSessionProperties();
        assertAllEquals(registeredProperty, propertyMetadataList);
    }

    @Test
    public void testConvertConnectorSession()
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        properties.put("test_bool_prop", false);
        properties.put("test_bool_prop_nullable", true);
        properties.put("test_int_prop", 1);
        properties.put("test_int_prop_nullable", 100);
        properties.put("test_long_prop", 1001L);
        properties.put("test_long_prop_nullable", 1000L);
        properties.put("test_double_prop", 100.1);
        properties.put("test_double_prop_nullable", 1.9);
        properties.put("test_string_prop", "another string");
        properties.put("test_string_prop_nullable", "This is a string");
        properties.put("test_additional_prop", "should be ignored?");

        ConnectorSession connectorSession = new TestingConnectorSession("user",
                Optional.of("test"),
                UTC_KEY,
                ENGLISH,
                1234,
                propertyMetadataList,
                properties.build(),
                new FeaturesConfig().isLegacyTimestamp());

        PrestoThriftSession thriftSession = sessionProperties.convertConnectorSession(connectorSession);
        assertEquals(thriftSession.getQueryId(), connectorSession.getQueryId());
        assertEquals(thriftSession.getUser(), "user");
        assertEquals(thriftSession.getSource(), "test");
        assertNull(thriftSession.getPrincipalName());
        assertEquals(thriftSession.getZoneId(), "UTC");
        assertEquals(thriftSession.getLocaleCode(), "en");
        assertEquals(thriftSession.getStartTime(), 1234);

        Map<String, PrestoThriftBlock> passedSessionProperties = thriftSession.getPropertyMap();
        assertEqualsIgnoreOrder(passedSessionProperties.keySet(), ImmutableSet.of("test_bool_prop", "test_bool_prop_nullable", "test_int_prop", "test_int_prop_nullable",
                "test_long_prop", "test_long_prop_nullable", "test_double_prop", "test_double_prop_nullable", "test_string_prop", "test_string_prop_nullable"));
        assertEquals((boolean) passedSessionProperties.get("test_bool_prop").getBooleanData().getSingleValue(), false);
        assertEquals((boolean) passedSessionProperties.get("test_bool_prop_nullable").getBooleanData().getSingleValue(), true);
        assertEquals((int) passedSessionProperties.get("test_int_prop").getIntegerData().getSingleValue(), 1);
        assertEquals((int) passedSessionProperties.get("test_int_prop_nullable").getIntegerData().getSingleValue(), 100);
        assertEquals((long) passedSessionProperties.get("test_long_prop").getBigintData().getSingleValue(), 1001L);
        assertEquals((long) passedSessionProperties.get("test_long_prop_nullable").getBigintData().getSingleValue(), 1000L);
        assertEquals(passedSessionProperties.get("test_double_prop").getDoubleData().getSingleValue(), 100.1);
        assertEquals(passedSessionProperties.get("test_double_prop_nullable").getDoubleData().getSingleValue(), 1.9);
        assertEquals(passedSessionProperties.get("test_string_prop").getVarcharData().getSingleValue(), "another string");
        assertEquals(passedSessionProperties.get("test_string_prop_nullable").getVarcharData().getSingleValue(), "This is a string");
    }

    @Test
    public void testConvertConnectorSessionIgnoreDefault()
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        properties.put("test_bool_prop", true);
        properties.put("test_int_prop", 100);
        properties.put("test_long_prop", 1000L);
        properties.put("test_double_prop", 1.9);
        properties.put("test_string_prop", "This is a string");
        properties.put("test_additional_prop", "should be ignored?");

        ConnectorSession connectorSession = new TestingConnectorSession("user",
                Optional.of("test"),
                UTC_KEY,
                ENGLISH,
                System.currentTimeMillis(),
                propertyMetadataList,
                properties.build(),
                new FeaturesConfig().isLegacyTimestamp());
        PrestoThriftSession thriftSession = sessionProperties.convertConnectorSession(connectorSession);
        Map<String, PrestoThriftBlock> passedSessionProperties = thriftSession.getPropertyMap();
        assertEqualsIgnoreOrder(passedSessionProperties.keySet(), ImmutableSet.of());
    }

    private void assertAllEquals(List<PropertyMetadata<?>> actual, List<PropertyMetadata<?>> expected)
    {
        assertEquals(actual.size(), expected.size());
        for (int i = 0; i < actual.size(); i++) {
            assertTrue(sameDefinition(actual.get(i), expected.get(i)));
        }
    }

    private boolean sameDefinition(PropertyMetadata<?> actual, PropertyMetadata<?> expected)
    {
        if (actual == expected) {
            return true;
        }
        return Objects.equals(actual.getName(), expected.getName())
                && Objects.equals(actual.getDescription(), expected.getDescription())
                && Objects.equals(actual.getJavaType(), expected.getJavaType())
                && Objects.equals(actual.getDefaultValue(), expected.getDefaultValue())
                && Objects.equals(actual.isHidden(), expected.isHidden());
    }
}
