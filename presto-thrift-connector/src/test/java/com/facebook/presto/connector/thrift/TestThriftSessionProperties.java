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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.doubleSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringSessionProperty;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestThriftSessionProperties
{
    private final ThriftSessionProperties sessionProperties = new ThriftSessionProperties(new TestingThriftSessionProperties());

    @Test
    public void testCreateHeaderFromSession()
    {
        Map<String, String> result = sessionProperties.createHeaderFromSession(
                createTestSession(
                        ImmutableMap.of(
                                "string_session_prop", "123",
                                "int_session_prop", 1,
                                "double_session_prop", 100.0,
                                "bool_session_prop", true)));
        Map<String, String> expected = ImmutableMap.of(
                "test_session_prop1", "123",
                "test_session_prop2", "1",
                "test_session_prop3", "100.0",
                "test_session_prop4", "true");
        assertEquals(result, expected);
    }

    @Test
    public void testIgnoreDefault()
    {
        Map<String, String> result = sessionProperties.createHeaderFromSession(
                createTestSession(
                        ImmutableMap.of(
                                "int_session_prop", 0,
                                "double_session_prop", 0.0,
                                "bool_session_prop", false)));

        assertTrue(result.isEmpty());
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testRejectUnknownType()
    {
        ThriftSessionProperties sessionProperties = new ThriftSessionProperties(() ->
                ImmutableMap.of(
                        "test_session_prop",
                        new PropertyMetadata<>("unsupported_typed_prop", "custom types are not supported", VARCHAR, TestingClass.class,
                                null, false, value -> new TestingClass((String) value), object -> object == null ? null : object.getValue())));
    }

    private ConnectorSession createTestSession(Map<String, Object> sessionValues)
    {
        return new TestingConnectorSession(
                "test",
                Optional.empty(),
                TimeZoneKey.UTC_KEY,
                Locale.ENGLISH,
                0,
                sessionProperties.getSessionProperties(),
                sessionValues,
                false);
    }

    private static class TestingClass
    {
        private final String value;

        public TestingClass(String value)
        {
            this.value = value;
        }

        String getValue()
        {
            return value;
        }

        @Override
        public String toString()
        {
            return value;
        }
    }

    private static class TestingThriftSessionProperties
            implements SessionPropertyProvider
    {
        @Override
        public Map<String, PropertyMetadata<?>> getHeaderProperties()
        {
            return ImmutableMap.of(
                    "test_session_prop1", stringSessionProperty("string_session_prop", "test string session prop", null, false),
                    "test_session_prop2", integerSessionProperty("int_session_prop", "test int session prop", 0, false),
                    "test_session_prop3", doubleSessionProperty("double_session_prop", "test double session prop", 0.0, false),
                    "test_session_prop4", booleanSessionProperty("bool_session_prop", "test boolean session prop", false, false));
        }
    }
}
