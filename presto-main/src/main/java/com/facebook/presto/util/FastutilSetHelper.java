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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.PrestoException;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.booleans.BooleanOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleHash;
import it.unimi.dsi.fastutil.doubles.DoubleOpenCustomHashSet;
import it.unimi.dsi.fastutil.longs.LongHash;
import it.unimi.dsi.fastutil.longs.LongOpenCustomHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenCustomHashSet;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.util.Collection;
import java.util.Set;

import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.HASH_CODE;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static java.lang.Boolean.TRUE;
import static java.lang.Math.toIntExact;

public final class FastutilSetHelper
{
    private FastutilSetHelper() {}

    @SuppressWarnings("unchecked")
    public static Set<?> toFastutilHashSet(Set<?> set, Type type, FunctionAndTypeManager functionAndTypeManager)
    {
        // 0.25 as the load factor is chosen because the argument set is assumed to be small (<10000),
        // and the return set is assumed to be read-heavy.
        // The performance of InCodeGenerator heavily depends on the load factor being small.
        Class<?> javaElementType = type.getJavaType();
        if (javaElementType == long.class) {
            return new LongOpenCustomHashSet((Collection<Long>) set, 0.25f, new LongStrategy(functionAndTypeManager, type));
        }
        if (javaElementType == double.class) {
            return new DoubleOpenCustomHashSet((Collection<Double>) set, 0.25f, new DoubleStrategy(functionAndTypeManager, type));
        }
        if (javaElementType == boolean.class) {
            return new BooleanOpenHashSet((Collection<Boolean>) set, 0.25f);
        }
        else if (!type.getJavaType().isPrimitive()) {
            return new ObjectOpenCustomHashSet(set, 0.25f, new ObjectStrategy(functionAndTypeManager, type));
        }
        else {
            throw new UnsupportedOperationException("Unsupported native type in set: " + type.getJavaType() + " with type " + type.getTypeSignature());
        }
    }

    public static boolean in(boolean booleanValue, BooleanOpenHashSet set)
    {
        return set.contains(booleanValue);
    }

    public static boolean in(double doubleValue, DoubleOpenCustomHashSet set)
    {
        return set.contains(doubleValue);
    }

    public static boolean in(long longValue, LongOpenCustomHashSet set)
    {
        return set.contains(longValue);
    }

    public static boolean in(Object objectValue, ObjectOpenCustomHashSet<?> set)
    {
        return set.contains(objectValue);
    }

    private static final class LongStrategy
            implements LongHash.Strategy
    {
        private final MethodHandle hashCodeHandle;
        private final MethodHandle equalsHandle;

        private LongStrategy(FunctionAndTypeManager functionAndTypeManager, Type type)
        {
            hashCodeHandle = functionAndTypeManager.getBuiltInScalarFunctionImplementation(functionAndTypeManager.resolveOperator(HASH_CODE, fromTypes(type))).getMethodHandle();
            equalsHandle = functionAndTypeManager.getBuiltInScalarFunctionImplementation(functionAndTypeManager.resolveOperator(EQUAL, fromTypes(type, type))).getMethodHandle();
        }

        @Override
        public int hashCode(long value)
        {
            try {
                return Long.hashCode((long) hashCodeHandle.invokeExact(value));
            }
            catch (Throwable t) {
                throwIfInstanceOf(t, Error.class);
                throwIfInstanceOf(t, PrestoException.class);
                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
            }
        }

        @Override
        public boolean equals(long a, long b)
        {
            try {
                Boolean result = (Boolean) equalsHandle.invokeExact(a, b);
                // FastutilHashSet is not intended be used for indeterminate values lookup
                verify(result != null, "result is null");
                return TRUE.equals(result);
            }
            catch (Throwable t) {
                throwIfInstanceOf(t, Error.class);
                throwIfInstanceOf(t, PrestoException.class);
                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
            }
        }
    }

    private static final class DoubleStrategy
            implements DoubleHash.Strategy
    {
        private final MethodHandle hashCodeHandle;
        private final MethodHandle equalsHandle;

        private DoubleStrategy(FunctionAndTypeManager functionAndTypeManager, Type type)
        {
            hashCodeHandle = functionAndTypeManager.getBuiltInScalarFunctionImplementation(functionAndTypeManager.resolveOperator(HASH_CODE, fromTypes(type))).getMethodHandle();
            equalsHandle = functionAndTypeManager.getBuiltInScalarFunctionImplementation(functionAndTypeManager.resolveOperator(EQUAL, fromTypes(type, type))).getMethodHandle();
        }

        @Override
        public int hashCode(double value)
        {
            try {
                return Long.hashCode((long) hashCodeHandle.invokeExact(value));
            }
            catch (Throwable t) {
                throwIfInstanceOf(t, Error.class);
                throwIfInstanceOf(t, PrestoException.class);
                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
            }
        }

        @Override
        public boolean equals(double a, double b)
        {
            try {
                Boolean result = (Boolean) equalsHandle.invokeExact(a, b);
                // FastutilHashSet is not intended be used for indeterminate values lookup
                verify(result != null, "result is null");
                return TRUE.equals(result);
            }
            catch (Throwable t) {
                throwIfInstanceOf(t, Error.class);
                throwIfInstanceOf(t, PrestoException.class);
                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
            }
        }
    }

    private static final class ObjectStrategy
            implements Hash.Strategy
    {
        private final MethodHandle hashCodeHandle;
        private final MethodHandle equalsHandle;

        private ObjectStrategy(FunctionAndTypeManager functionAndTypeManager, Type type)
        {
            hashCodeHandle = functionAndTypeManager.getBuiltInScalarFunctionImplementation(functionAndTypeManager.resolveOperator(HASH_CODE, fromTypes(type)))
                    .getMethodHandle()
                    .asType(MethodType.methodType(long.class, Object.class));
            equalsHandle = functionAndTypeManager.getBuiltInScalarFunctionImplementation(functionAndTypeManager.resolveOperator(EQUAL, fromTypes(type, type)))
                    .getMethodHandle()
                    .asType(MethodType.methodType(Boolean.class, Object.class, Object.class));
        }

        @Override
        public int hashCode(Object value)
        {
            try {
                return toIntExact(Long.hashCode((long) hashCodeHandle.invokeExact(value)));
            }
            catch (Throwable t) {
                throwIfInstanceOf(t, Error.class);
                throwIfInstanceOf(t, PrestoException.class);
                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
            }
        }

        @Override
        public boolean equals(Object a, Object b)
        {
            try {
                Boolean result = (Boolean) equalsHandle.invokeExact(a, b);
                return TRUE.equals(result);
            }
            catch (Throwable t) {
                throwIfInstanceOf(t, Error.class);
                throwIfInstanceOf(t, PrestoException.class);
                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
            }
        }
    }
}
