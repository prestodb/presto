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

package io.prestosql.spi.block;

import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.prestosql.spi.block.MethodHandleUtil.compose;
import static io.prestosql.spi.block.MethodHandleUtil.methodHandle;
import static org.testng.Assert.assertEquals;

public class TestMethodHandleUtil
{
    // Each custom type in this test is effectively a number.
    // All method handles in this test returns the product of all input parameters.
    // Each method handles has distinct input types and return type.

    // The composed function is invoked once to verify that:
    // * The composed function type is expected
    // * Each argument is multiplied into the product exactly once. (by using prime numbers as input)

    @Test
    public void testCompose2()
            throws Throwable
    {
        MethodHandle fUS2R = methodHandle(TestMethodHandleUtil.class, "fUS2R", U.class, S1.class, S2.class);
        MethodHandle fT2U = methodHandle(TestMethodHandleUtil.class, "fT2U", T1.class, T2.class);
        MethodHandle composed = compose(fUS2R, fT2U);
        assertEquals((R) composed.invokeExact(new T1(2), new T2(3), new S1(5), new S2(7)), new R(210));
    }

    @Test
    public void testCompose2withoutS()
            throws Throwable
    {
        MethodHandle fU2R = methodHandle(TestMethodHandleUtil.class, "fU2R", U.class);
        MethodHandle fT2U = methodHandle(TestMethodHandleUtil.class, "fT2U", T1.class, T2.class);
        MethodHandle composed = compose(fU2R, fT2U);
        assertEquals((R) composed.invokeExact(new T1(2), new T2(3)), new R(6));
    }

    @Test
    public void testCompose3()
            throws Throwable
    {
        MethodHandle fUV2R = methodHandle(TestMethodHandleUtil.class, "fUV2R", U.class, V.class);
        MethodHandle fS2U = methodHandle(TestMethodHandleUtil.class, "fS2U", S1.class, S2.class);
        MethodHandle fT2V = methodHandle(TestMethodHandleUtil.class, "fT2V", T1.class, T2.class);
        MethodHandle composed = compose(fUV2R, fS2U, fT2V);
        assertEquals((R) composed.invokeExact(new S1(2), new S2(3), new T1(5), new T2(7)), new R(210));
    }

    public static R fU2R(U u)
    {
        return new R(u.getValue());
    }

    public static R fUS2R(U u, S1 s1, S2 s2)
    {
        return new R(u.getValue() * s1.getValue() * s2.getValue());
    }

    public static R fUV2R(U u, V v)
    {
        return new R(u.getValue() * v.getValue());
    }

    public static U fT2U(T1 t1, T2 t2)
    {
        return new U(t1.getValue() * t2.getValue());
    }

    public static U fS2U(S1 s1, S2 s2)
    {
        return new U(s1.getValue() * s2.getValue());
    }

    public static V fT2V(T1 t1, T2 t2)
    {
        return new V(t1.getValue() * t2.getValue());
    }

    public static String squareBracket(String s)
    {
        return "[" + s + "]";
    }

    public static String squareBracket(String s, double d)
    {
        return "[" + s + "," + ((long) d) + "]";
    }

    public static String curlyBracket(String s, char c)
    {
        return "{" + s + "=" + c + "}";
    }

    public static double sum(long x, int c)
    {
        return (double) x + c;
    }

    private static class Base
    {
        private final int value;

        public Base(int value)
        {
            this.value = value;
        }

        public int getValue()
        {
            return value;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Base base = (Base) o;
            return value == base.value;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(value);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("value", value)
                    .toString();
        }
    }

    private static class U
            extends Base
    {
        public U(int value)
        {
            super(value);
        }
    }

    private static class V
            extends Base
    {
        public V(int value)
        {
            super(value);
        }
    }

    private static class R
            extends Base
    {
        public R(int value)
        {
            super(value);
        }
    }

    private static class S1
            extends Base
    {
        public S1(int value)
        {
            super(value);
        }
    }

    private static class S2
            extends Base
    {
        public S2(int value)
        {
            super(value);
        }
    }

    private static class T1
            extends Base
    {
        public T1(int value)
        {
            super(value);
        }
    }

    private static class T2
            extends Base
    {
        public T2(int value)
        {
            super(value);
        }
    }
}
