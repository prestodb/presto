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
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.doubleSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.longSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringSessionProperty;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestThriftSessionProperties
{
    private final ThriftSessionProperties sessionProperties = new ThriftSessionProperties(new TestingThriftSessionProperties());

    @Test
    public void testCreateHeaderFromSession()
    {
        Map<String, Object> sessionValues = ImmutableMap.<String, Object>builder()
                .put("string_session_prop", "123")
                .put("int_session_prop", 1)
                .put("long_session_prop", 13L)
                .put("double_session_prop", 100.0)
                .put("bool_session_prop", true)
                .build();

        Map<String, String> result = sessionProperties.toHeader(createTestSession(sessionValues));
        Map<String, String> expected = ImmutableMap.<String, String>builder()
                .put("test_session_prop1", "123")
                .put("test_session_prop2", "1")
                .put("test_session_prop3", "13")
                .put("test_session_prop4", "100.0")
                .put("test_session_prop5", "true")
                .build();

        assertEquals(result, expected);
    }

    @Test
    public void testIgnoreDefault()
    {
        Map<String, Object> sessionValues = ImmutableMap.<String, Object>builder()
                .put("int_session_prop", -1)
                .put("long_session_prop", 1)
                .put("double_session_prop", 0.01)
                .put("bool_session_prop", false)
                .put("another_string_session_prop", "123")
                .build();
        Map<String, String> result = sessionProperties.toHeader(createTestSession(sessionValues));

        assertTrue(result.isEmpty());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testRejectUnknownType()
    {
        ThriftSessionProperties sessionProperties = new ThriftSessionProperties(() ->
                ImmutableMap.of(
                        "test_session_prop",
                        new PropertyMetadata<>(
                                "unsupported_typed_prop",
                                "complex types are not supported",
                                BIGINT,
                                AtomicLong.class,
                                null,
                                false,
                                value -> new AtomicLong(((Number) value).intValue()),
                                object -> object == null ? null : object.toString())));
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

    private static class TestingThriftSessionProperties
            implements SessionPropertyProvider
    {
        @Override
        public Map<String, PropertyMetadata<?>> getHeaderProperties()
        {
            return ImmutableMap.<String, PropertyMetadata<?>>builder()
                    .put("test_session_prop1", stringSessionProperty("string_session_prop", "test string session prop", null, false))
                    .put("test_session_prop2", integerSessionProperty("int_session_prop", "test int session prop", -1, false))
                    .put("test_session_prop3", longSessionProperty("long_session_prop", "test long session prop", 1L, false))
                    .put("test_session_prop4", doubleSessionProperty("double_session_prop", "test double session prop", 0.01, false))
                    .put("test_session_prop5", booleanSessionProperty("bool_session_prop", "test boolean session prop", false, false))
                    .put("test_session_prop6", stringSessionProperty("another_string_session_prop", "another test string session prop", "123", false))
                    .build();
        }
    }
}
