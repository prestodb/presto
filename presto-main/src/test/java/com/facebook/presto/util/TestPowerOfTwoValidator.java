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

import org.apache.bval.jsr.ApacheValidationProvider;
import org.testng.annotations.Test;

import javax.validation.ConstraintValidatorContext;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;

import java.util.Set;

import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPowerOfTwoValidator
{
    private static final Validator VALIDATOR = Validation.byProvider(ApacheValidationProvider.class).configure().buildValidatorFactory().getValidator();

    @Test
    public void testValidator()
    {
        PowerOfTwoValidator validator = new PowerOfTwoValidator();
        validator.initialize(new MockPowerOfTwo());

        assertTrue(validator.isValid(1, new MockContext()));
        assertTrue(validator.isValid(2, new MockContext()));
        assertTrue(validator.isValid(64, new MockContext()));
        assertFalse(validator.isValid(0, new MockContext()));
        assertFalse(validator.isValid(3, new MockContext()));
        assertFalse(validator.isValid(99, new MockContext()));
        assertFalse(validator.isValid(-1, new MockContext()));
        assertFalse(validator.isValid(-2, new MockContext()));
        assertFalse(validator.isValid(-4, new MockContext()));
    }

    @Test
    public void testAllowsNullPowerOfTwoAnnotation()
    {
        VALIDATOR.validate(new NullPowerOfTwoAnnotation());
    }

    @Test
    public void testPassesValidation()
    {
        ConstrainedPowerOfTwo object = new ConstrainedPowerOfTwo(128);
        Set<ConstraintViolation<ConstrainedPowerOfTwo>> violations = VALIDATOR.validate(object);
        assertTrue(violations.isEmpty());
    }

    @Test
    public void testFailsValidation()
    {
        ConstrainedPowerOfTwo object = new ConstrainedPowerOfTwo(11);
        Set<ConstraintViolation<ConstrainedPowerOfTwo>> violations = VALIDATOR.validate(object);
        assertEquals(violations.size(), 2);

        for (ConstraintViolation<ConstrainedPowerOfTwo> violation : violations) {
            assertInstanceOf(violation.getConstraintDescriptor().getAnnotation(), PowerOfTwo.class);
        }
    }

    private static class MockContext
            implements ConstraintValidatorContext
    {
        @Override
        public void disableDefaultConstraintViolation()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getDefaultConstraintMessageTemplate()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConstraintViolationBuilder buildConstraintViolationWithTemplate(String s)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T unwrap(Class<T> type)
        {
            throw new UnsupportedOperationException();
        }
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
        public int getConstrainedByPowerOfTwoUnboxed()
        {
            return value;
        }

        @PowerOfTwo
        public Integer getConstrainedByPowerOfTwoBoxed()
        {
            return value;
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public static class NullPowerOfTwoAnnotation
    {
        @PowerOfTwo
        public static Integer getConstrainedByPowerOfTwo()
        {
            return null;
        }
    }
}
