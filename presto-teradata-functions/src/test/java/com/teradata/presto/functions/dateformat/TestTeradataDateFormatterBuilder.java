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
package com.teradata.presto.functions.dateformat;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestTeradataDateFormatterBuilder
{
    private TeradataDateFormatterBuilder builder;

    @BeforeClass
    public void setUp()
    {
        builder = new TeradataDateFormatterBuilder();
    }

    @Test
    public void testFormatterBuilder()
    {
        testFormat("yyyy/mm/dd", "1988/04/08", new DateTime(1988, 4, 8, 0, 0));
    }

    private void testFormat(String format, String dateString, DateTime expected)
    {
        DateTimeFormatter formatter = builder.createDateTimeFormatter(format);
        assertEquals(formatter.parseDateTime(dateString), expected);
    }
}
