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

package com.facebook.presto.pinot;

import org.testng.annotations.Test;

import static com.facebook.presto.pinot.PinotUtils.parseTimestamp;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestPinotUtils
{
    @Test
    public void testParseTimestamp()
    {
        long epoch = parseTimestamp("1.672528152E12");
        assertEquals(epoch, 1672528152000L);
        long epoch2 = parseTimestamp("1652374863000");
        assertEquals(epoch2, 1652374863000L);
        long epoch3 = parseTimestamp("2022-05-12 10:48:06.5");
        assertEquals(epoch3, 1652370486500L);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testParseTimestampWithException()
    {
        parseTimestamp("some string");
        fail("Should throw exception for parsing wrong timestamp");
    }
}
