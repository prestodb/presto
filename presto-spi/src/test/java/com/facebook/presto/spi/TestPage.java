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
package com.facebook.presto.spi;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestPage
{
    @Test
    public void testGetRegion()
            throws Exception
    {
        assertEquals(new Page(10).getRegion(5, 5).getPositionCount(), 5);
    }

    @Test
    public void testGetEmptyRegion()
            throws Exception
    {
        assertEquals(new Page(0).getRegion(0, 0).getPositionCount(), 0);
        assertEquals(new Page(10).getRegion(5, 0).getPositionCount(), 0);
    }

    @Test(expectedExceptions = IndexOutOfBoundsException.class, expectedExceptionsMessageRegExp = "Invalid position .* in page with .* positions")
    public void testGetRegionExceptions()
            throws Exception
    {
        new Page(0).getRegion(1, 1);
    }

    @Test
    public void testGetRegionFromNoColumnPage()
            throws Exception
    {
        assertEquals(new Page(100).getRegion(0, 10).getPositionCount(), 10);
    }
}
