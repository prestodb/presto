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
package io.prestosql.plugin.kudu.properties;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertEquals;

public class TestRangePartitionSerialization
{
    private String[] testInputs = new String[] {
            "{\"lower\":1,\"upper\":null}",
            "{\"lower\":12345678901234567890,\"upper\":1.234567890123457E-13}",
            "{\"lower\":\"abc\",\"upper\":\"abf\"}",
            "{\"lower\":false,\"upper\":true}",
            "{\"lower\":\"ABCD\",\"upper\":\"ABCDEF\"}",
            "{\"lower\":[\"ABCD\",1,0],\"upper\":[\"ABCD\",13,0]}",
    };

    @Test
    public void testDeserializationSerialization()
            throws IOException
    {
        ObjectMapper mapper = new ObjectMapper();

        for (String input : testInputs) {
            RangePartition partition = mapper.readValue(input, RangePartition.class);

            String serialized = mapper.writeValueAsString(partition);
            assertEquals(serialized, input);
        }
    }
}
