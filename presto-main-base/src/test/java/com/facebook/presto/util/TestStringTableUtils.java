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
package com.facebook.presto.util;

import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestStringTableUtils
{
    @Test
    public void testGetShortestTableStringFormatSimpleSuccess()
    {
        List<List<String>> table = Arrays.asList(
                Arrays.asList("Header1", "Header2", "Headr3"),
                Arrays.asList("Value1", "Value2", "Value3"),
                Arrays.asList("LongValue1", "SVal2", "SVal3"));

        assertEquals(
                StringTableUtils.getShortestTableStringFormat(table),
                "| %-10s | %-7s | %-6s |");
    }

    @Test
    public void testGetShortestTableStringFormatBadInput()
    {
        List<List<String>> table = Arrays.asList(
                Arrays.asList("Header1", "Header2", "Headr3"),
                Arrays.asList("Value1", "Value2"),
                Arrays.asList("LongValue1", "SVal2", "SVal3"));

        assertThrows(
                IllegalArgumentException.class,
                () -> StringTableUtils.getShortestTableStringFormat(table));
    }

    @Test
    public void testGetTableStringsSimpleSuccess()
    {
        List<List<String>> table = Arrays.asList(
                Arrays.asList("Header1", "Header2", "Headr3"),
                Arrays.asList("Value1", "Value2", "Value3"),
                Arrays.asList("LongValue1", "SVal2", "SVal3"));

        assertEquals(
                StringTableUtils.getTableStrings(table),
                Arrays.asList(
                        "| Header1    | Header2 | Headr3 |",
                        "| Value1     | Value2  | Value3 |",
                        "| LongValue1 | SVal2   | SVal3  |"));
    }
}
