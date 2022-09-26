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
package com.facebook.presto.resourceGroups;

import org.apache.logging.log4j.core.util.CronExpression;
import org.testng.annotations.Test;

import java.time.ZoneId;

import static org.apache.logging.log4j.core.util.CronExpression.isValidExpression;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestSelectorSchedule
{
    @Test
    public void testInvalidInput()
    {
        /**
         * No time zone provided.
         */
        try {
            new SelectorSchedule("* * * * * *");
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) { }

        /**
         * No schedule expression provided.
         */
        try {
            new SelectorSchedule("TZ=America/Los_Angeles");
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) { }

        /**
         * Illegal schedule expression
         */
        try {
            new SelectorSchedule("TZ=America/Los_Angeles; not-an-expression");
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) { }

        /**
         * Illegal schedule expression; 6 required fields.
         */
        try {
            new SelectorSchedule("TZ=America/Los_Angeles; * * * * *");
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) { }

        /**
         * Illegal schedule expression; day-of-month and day-of-week specified.
         */
        try {
            new SelectorSchedule("TZ=America/Los_Angeles; * * * ? * ?");
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) { }

        /**
         * Invalid time zone provided.
         */
        try {
            new SelectorSchedule("TZ=Random/City; * * * * * *");
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) { }
    }

    @Test
    public void testValidInput()
    {
        SelectorSchedule testSelector1 = new SelectorSchedule("TZ=Asia/Shanghai; * * * ? * *");
        assertSelectorSchedule(testSelector1, ZoneId.of("Asia/Shanghai"), "* * * ? * *");

        SelectorSchedule testSelector2 = new SelectorSchedule("TZ=Africa/Cairo; * * * * * ?");
        assertSelectorSchedule(testSelector2, ZoneId.of("Africa/Cairo"), "* * * * * ?");

        SelectorSchedule testSelector3 = new SelectorSchedule("TZ=Pacific/Auckland; 0-59 0-59 0-23 ? 1-12 1-7");
        assertSelectorSchedule(testSelector3, ZoneId.of("Pacific/Auckland"), "0-59 0-59 0-23 ? 1-12 1-7");

        SelectorSchedule testSelector4 = new SelectorSchedule("TZ=Australia/Sydney; 59-0 59-0 23-0 ? DEC-JAN SAT-SUN");
        assertSelectorSchedule(testSelector4, ZoneId.of("Australia/Sydney"), "59-0 59-0 23-0 ? DEC-JAN SAT-SUN");

        SelectorSchedule testSelector5 = new SelectorSchedule("TZ=Asia/Ho_Chi_Minh; * * * ? * * 1970-2199");
        assertSelectorSchedule(testSelector5, ZoneId.of("Asia/Ho_Chi_Minh"), "* * * ? * * 1970-2199");
    }

    private void assertSelectorSchedule(SelectorSchedule selectorSchedule, ZoneId expectedTimeZone, String expectedExpression)
    {
        ZoneId actualTimeZone = selectorSchedule.getTimeZoneId();
        CronExpression scheduleExpression = selectorSchedule.getExpression();
        assertEquals(actualTimeZone, expectedTimeZone);
        isValidExpression(scheduleExpression.toString());
        assertTrue(scheduleExpression.getTimeZone().toZoneId().equals(expectedTimeZone));
        assertEquals(scheduleExpression.toString(), expectedExpression);
    }
}
