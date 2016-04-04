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
package com.facebook.presto.event.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import io.airlift.json.ObjectMapperProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class TestQueryMonitor
{
    @Test
    public void testToJsonWithLengthLimit()
    {
        TestClass testClass = new TestClass(Strings.repeat("a", 1000));
        ObjectMapper objectMapper = new ObjectMapperProvider().get();
        assertNull(QueryMonitor.toJsonWithLengthLimit(objectMapper, testClass, 1011));
        assertNotNull(QueryMonitor.toJsonWithLengthLimit(objectMapper, testClass, 1012));
    }

    public static class TestClass
    {
        private final String value;

        @JsonCreator
        public TestClass(@JsonProperty("value") String value)
        {
            this.value = value;
        }

        @JsonProperty
        public String getValue()
        {
            return value;
        }
    }
}
