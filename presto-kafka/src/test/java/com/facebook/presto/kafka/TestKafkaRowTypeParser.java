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
package com.facebook.presto.kafka;

import io.airlift.log.Logger;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

public class TestKafkaRowTypeParser
{
    private static final Logger log = Logger.get("TestQueries");

    @Test
    public void testToList()
    {
        String testString = "VARCHAR,ARRAY<VARCHAR>,INTEGER";
        List<String> result = KafkaRowTypeParser.parseParamsString(testString);
        assertEquals(result.size(), 3);
        assertEquals(result.get(0), "VARCHAR");
        assertEquals(result.get(1), "ARRAY<VARCHAR>");
        assertEquals(result.get(2), "INTEGER");
    }

    @Test
    public void testParse()
    {
        String testString = "ROW<VARCHAR,ARRAY<VARCHAR>,INTEGER>('a','b','c')";
        KafkaRowTypeParser.parse(testString);

        assertEquals(KafkaRowTypeParser.typeStrings.size(), 3);
        assertEquals(KafkaRowTypeParser.typeStrings.get(0), "VARCHAR");
        assertEquals(KafkaRowTypeParser.typeStrings.get(1), "ARRAY<VARCHAR>");
        assertEquals(KafkaRowTypeParser.typeStrings.get(2), "INTEGER");

        assertEquals(KafkaRowTypeParser.names.size(), 3);
        assertEquals(KafkaRowTypeParser.names.get(0), "a");
        assertEquals(KafkaRowTypeParser.names.get(1), "b");
        assertEquals(KafkaRowTypeParser.names.get(2), "c");
    }
}
