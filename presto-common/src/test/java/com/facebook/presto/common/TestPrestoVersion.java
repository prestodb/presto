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
package com.facebook.presto.common;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPrestoVersion
{
    private static final PrestoVersion VERSION_274_20220517_171619_22 = new PrestoVersion("0.274-20220517.171619-22");
    private static final PrestoVersion VERSION_274_20220518_025123_29 = new PrestoVersion("0.274-20220518.025123-29");
    private static final PrestoVersion VERSION_274_SNAPSHOT_5d0ba93 = new PrestoVersion("0.274-SNAPSHOT-5d0ba93");
    private static final PrestoVersion VERSION_273 = new PrestoVersion("0.273");
    private static final PrestoVersion VERSION_274 = new PrestoVersion("0.274");
    private static final PrestoVersion VERSION_274_1 = new PrestoVersion("0.274.1");
    private static final PrestoVersion VERSION_274_2 = new PrestoVersion("0.274.2");

    @Test
    public void testPrestoEquals()
    {
        assertTrue(VERSION_274_20220517_171619_22.equals(VERSION_274_20220517_171619_22));
        assertTrue(VERSION_274_1.equals(new PrestoVersion("0.274.1")));
        assertFalse(VERSION_274_1.equals(VERSION_274));
        assertFalse(VERSION_274_20220517_171619_22.equals(null));
    }

    @Test
    public void testLessThan()
    {
        //Comparing major versions
        assertFalse(VERSION_274_1.lessThan(VERSION_273));

        // Comparing Snapshot and non-Snapshot versions
        assertTrue(VERSION_274_20220517_171619_22.lessThan(VERSION_274));
        assertTrue(VERSION_274_SNAPSHOT_5d0ba93.lessThan(VERSION_274));
        assertTrue(VERSION_274_SNAPSHOT_5d0ba93.lessThan(VERSION_274_1));

        //Comparing snapshot versions
        assertFalse(VERSION_274_20220518_025123_29.lessThan(VERSION_274_20220517_171619_22));

        // Comparing equal versions
        assertFalse(VERSION_274_SNAPSHOT_5d0ba93.lessThan(VERSION_274_SNAPSHOT_5d0ba93));

        // Comparing major and minor versions
        assertTrue(VERSION_274.lessThan(VERSION_274_1));
        assertFalse(VERSION_274_1.lessThan(VERSION_274));

        //comparing minor versions
        assertFalse(VERSION_274_2.lessThan(VERSION_274_1));
    }

    @Test
    public void testLessThanOrEqualTo()
    {
        assertTrue(VERSION_274_20220517_171619_22.lessThanOrEqualTo(VERSION_274));
        assertTrue(VERSION_274_SNAPSHOT_5d0ba93.lessThanOrEqualTo(VERSION_274));
        assertTrue(VERSION_274.lessThanOrEqualTo(VERSION_274_1));
        assertTrue(VERSION_274_SNAPSHOT_5d0ba93.lessThanOrEqualTo(VERSION_274_1));

        //The following values should be equal
        assertTrue(VERSION_274_SNAPSHOT_5d0ba93.lessThanOrEqualTo(VERSION_274_SNAPSHOT_5d0ba93));
        assertTrue(VERSION_274.lessThanOrEqualTo(VERSION_274));
    }

    @Test
    public void testGreaterThanOrEqualTo()
    {
        assertTrue(VERSION_274.greaterThanOrEqualTo(VERSION_274_20220517_171619_22));
        assertTrue(VERSION_274.greaterThanOrEqualTo(VERSION_274_SNAPSHOT_5d0ba93));
        assertTrue(VERSION_274_1.greaterThanOrEqualTo(VERSION_274));
        assertTrue(VERSION_274_1.greaterThanOrEqualTo(VERSION_274_SNAPSHOT_5d0ba93));

        //The following values should be equal
        assertTrue(VERSION_274_SNAPSHOT_5d0ba93.greaterThanOrEqualTo(VERSION_274_SNAPSHOT_5d0ba93));
        assertTrue(VERSION_274.greaterThanOrEqualTo(VERSION_274));
    }

    @Test
    public void testGreaterThan()
    {
        //Comparing major versions
        assertTrue(VERSION_274_1.greaterThan(VERSION_273));

        // Comparing Snapshot and non-Snapshot versions
        assertTrue(VERSION_274.greaterThan(VERSION_274_20220517_171619_22));
        assertFalse(VERSION_274_20220517_171619_22.greaterThan(VERSION_274));
        assertFalse(VERSION_274_SNAPSHOT_5d0ba93.greaterThan(VERSION_274));
        assertFalse(VERSION_274_SNAPSHOT_5d0ba93.greaterThan(VERSION_274_1));

        //Comparing snapshot versions
        assertFalse(VERSION_274_20220517_171619_22.greaterThan(VERSION_274_20220518_025123_29));
        assertTrue(VERSION_274_20220518_025123_29.greaterThan(VERSION_274_20220517_171619_22));

        // Comparing equal versions
        assertFalse(VERSION_274_SNAPSHOT_5d0ba93.greaterThan(VERSION_274_SNAPSHOT_5d0ba93));

        // Comparing major and minor versions
        assertFalse(VERSION_274.greaterThan(VERSION_274_1));
        assertTrue(VERSION_274_1.greaterThan(VERSION_274));
    }

    @Test
    public void testVersionValidity()
    {
        PrestoVersion minVersion = new PrestoVersion("0.282");
        PrestoVersion maxVersion = new PrestoVersion("0.292");
        assertFalse(minVersion.lessThanOrEqualTo(VERSION_274_20220517_171619_22) && maxVersion.greaterThanOrEqualTo(VERSION_274_20220517_171619_22));
    }
}
