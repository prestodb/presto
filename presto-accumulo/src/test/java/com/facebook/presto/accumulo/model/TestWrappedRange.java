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
package com.facebook.presto.accumulo.model;

import org.apache.accumulo.core.data.Range;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestWrappedRange
{
    @Test
    public void testJsonRoundTrip()
            throws Exception
    {
        Range exact = new Range("foo");
        Range range = new Range("bar", "foo");
        Range exclusiveRange = new Range("asiago", false, "bagel", false);

        assertEquals(WrappedRange.fromBytes(new WrappedRange(exact).toBytes()).getRange(), exact);
        assertEquals(WrappedRange.fromBytes(new WrappedRange(range).toBytes()).getRange(), range);
        assertEquals(WrappedRange.fromBytes(new WrappedRange(exclusiveRange).toBytes()).getRange(), exclusiveRange);
    }
}
