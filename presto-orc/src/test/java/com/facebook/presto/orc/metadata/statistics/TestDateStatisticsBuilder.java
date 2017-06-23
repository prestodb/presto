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
package com.facebook.presto.orc.metadata.statistics;

import org.testng.annotations.Test;

import static com.facebook.presto.orc.metadata.statistics.AbstractStatisticsBuilderTest.StatisticsType.DATE;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.MIN_VALUE;
import static org.testng.Assert.fail;

public class TestDateStatisticsBuilder
        extends AbstractStatisticsBuilderTest<DateStatisticsBuilder, Integer>
{
    public TestDateStatisticsBuilder()
    {
        super(DATE, DateStatisticsBuilder::new, DateStatisticsBuilder::addValue);
    }

    @Test
    public void testMinMaxValues()
    {
        assertMinMaxValues(0, 0);
        assertMinMaxValues(42, 42);
        assertMinMaxValues(MIN_VALUE, MIN_VALUE);
        assertMinMaxValues(MAX_VALUE, MAX_VALUE);

        assertMinMaxValues(0, 42);
        assertMinMaxValues(42, 42);
        assertMinMaxValues(MIN_VALUE, 42);
        assertMinMaxValues(42, MAX_VALUE);
        assertMinMaxValues(MIN_VALUE, MAX_VALUE);
    }

    @Test
    public void testValueOutOfRange()
    {
        try {
            new DateStatisticsBuilder().addValue(MAX_VALUE + 1L);
            fail("Expected ArithmeticException");
        }
        catch (ArithmeticException expected) {
        }

        try {
            new DateStatisticsBuilder().addValue(MIN_VALUE - 1L);
            fail("Expected ArithmeticException");
        }
        catch (ArithmeticException expected) {
        }
    }
}
