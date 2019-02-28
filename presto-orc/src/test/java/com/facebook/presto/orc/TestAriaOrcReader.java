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
package com.facebook.presto.orc;

import org.testng.annotations.Test;

@Test
public class TestAriaOrcReader
        extends AbstractTestOrcReader
{
    public TestAriaOrcReader()
    {
        super(OrcTester.ariaOrcTester());
    }

    @Test
    @Override
    public void testBooleanSequence() {}

    @Test
    @Override
    public void testLongSequence() {}

    @Test
    @Override
    public void testNegativeLongSequence() {}

    @Test
    @Override
    public void testLongSequenceWithHoles() {}

    @Test
    @Override
    public void testLongDirect() {}

    @Test
    @Override
    public void testLongDirect2() {}

    @Test
    @Override
    public void testLongShortRepeat() {}

    @Test
    @Override
    public void testLongPatchedBase() {}

    @Test
    @Override
    public void testLongStrideDictionary() {}

    @Test
    @Override
    public void testFloatSequence() {}

    @Test
    @Override
    public void testFloatNaNInfinity() {}

    @Test
    @Override
    public void testDecimalSequence() {}
}
