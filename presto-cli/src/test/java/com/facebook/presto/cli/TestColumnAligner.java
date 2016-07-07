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
package com.facebook.presto.cli;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;

public class TestColumnAligner
{
    private static final String HEADER = "test";
    private static final List<String> INPUT_VALUES = ImmutableList.of("1123123123324234", "123", "12546456", "45", "6785467467", "67", "6", "6475674567"); // max length is 16
    private static final List<String> RAW_OUTPUT = ImmutableList.<String>builder()
            .add(HEADER)
            .addAll(INPUT_VALUES)
            .build();

    @Test
    public void testMinimalLength()
            throws Exception
    {
        testColumnAligner(8, 0, "%16s");
    }

    @Test
    public void testLargeMinimalLength()
            throws Exception
    {
        testColumnAligner(20, 0, "%20s");
    }

    @Test
    public void testMargin()
            throws Exception
    {
        testColumnAligner(8, 2, "%18s");
    }

    private static void testColumnAligner(int minimalLength, int leftMargin, String expectedFormat)
    {
        ColumnAligner columnAligner = new ColumnAligner(HEADER, minimalLength, leftMargin);
        for (String value : INPUT_VALUES) {
            columnAligner.feedValue(value);
        }

        List<String> expected = alignedValues(expectedFormat, RAW_OUTPUT);
        assertEquals(columnAligner.buildColumn(), expected);
    }

    private static List<String> alignedValues(String format, List<String> values)
    {
        return values.stream()
                .map(value -> String.format(format, value))
                .collect(Collectors.toList());
    }
}
