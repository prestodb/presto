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

import static com.facebook.presto.util.MoreMath.roundToDouble;
import static java.lang.Double.NaN;
import static java.lang.Math.nextUp;
import static org.testng.Assert.assertEquals;

public class TestMoreMath
{
    @Test
    public void testRoundToDouble()
    {
        assertEquals(roundToDouble(NaN), NaN);
        assertEquals(roundToDouble(0.0), 0.0);
        assertEquals(roundToDouble(nextUp(0.0)), 0.0);
        assertEquals(roundToDouble(0.1), 0.0);
        assertEquals(roundToDouble(0.4), 0.0);
        assertEquals(roundToDouble(0.5), 1.0);
        assertEquals(roundToDouble(0.99), 1.0);
        assertEquals(roundToDouble(1.09), 1.0);
        assertEquals(roundToDouble(nextUp(1.0)), 1.0);
    }
}
