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
package io.prestosql.util;

import org.testng.annotations.Test;

import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;

public class TestPowerOfTwoValidator
{
    @Test
    public void testValidator()
    {
        assertValid(1);
        assertValid(2);
        assertValid(64);
        assertInvalid(0);
        assertInvalid(3);
        assertInvalid(99);
        assertInvalid(-1);
        assertInvalid(-2);
        assertInvalid(-4);
    }

    @Test
    public void testAllowsNullPowerOfTwoAnnotation()
    {
        assertValidates(new NullPowerOfTwoAnnotation());
    }

    private static void assertValid(int value)
    {
        assertValidates(new ConstrainedPowerOfTwo(value));
    }

    private static void assertInvalid(int value)
    {
        Object object = new ConstrainedPowerOfTwo(value);
        assertFailsValidation(object, "unboxed", "is not a power of two", PowerOfTwo.class);
        assertFailsValidation(object, "boxed", "is not a power of two", PowerOfTwo.class);
    }

    @SuppressWarnings("UnusedDeclaration")
    public static class ConstrainedPowerOfTwo
    {
        private final int value;

        public ConstrainedPowerOfTwo(int value)
        {
            this.value = value;
        }

        @PowerOfTwo
        public int getUnboxed()
        {
            return value;
        }

        @PowerOfTwo
        public Integer getBoxed()
        {
            return value;
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public static class NullPowerOfTwoAnnotation
    {
        @PowerOfTwo
        public Integer getNull()
        {
            return null;
        }
    }
}
