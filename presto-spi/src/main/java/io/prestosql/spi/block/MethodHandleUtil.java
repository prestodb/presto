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

import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.Type;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.lang.invoke.MethodType.methodType;

public final class MethodHandleUtil
{
    private static final MethodHandle GET_LONG = methodHandle(Type.class, "getLong", Block.class, int.class);
    private static final MethodHandle GET_DOUBLE = methodHandle(Type.class, "getDouble", Block.class, int.class);
    private static final MethodHandle GET_BOOLEAN = methodHandle(Type.class, "getBoolean", Block.class, int.class);
    private static final MethodHandle GET_SLICE = methodHandle(Type.class, "getSlice", Block.class, int.class);
    private static final MethodHandle GET_BLOCK = methodHandle(Type.class, "getObject", Block.class, int.class).asType(methodType(Block.class, Type.class, Block.class, int.class));

    private static final MethodHandle WRITE_LONG = methodHandle(Type.class, "writeLong", BlockBuilder.class, long.class);
    private static final MethodHandle WRITE_DOUBLE = methodHandle(Type.class, "writeDouble", BlockBuilder.class, double.class);
    private static final MethodHandle WRITE_BOOLEAN = methodHandle(Type.class, "writeBoolean", BlockBuilder.class, boolean.class);
    private static final MethodHandle WRITE_SLICE = methodHandle(Type.class, "writeSlice", BlockBuilder.class, Slice.class);
    private static final MethodHandle WRITE_BLOCK = methodHandle(Type.class, "writeObject", BlockBuilder.class, Object.class).asType(methodType(void.class, Type.class, BlockBuilder.class, Block.class));

    private MethodHandleUtil()
    {
    }

    /**
     * @param f (U, S1, S2, ..., Sm)R
     * @param g (T1, T2, ..., Tn)U
     * @return (T1, T2, ..., Tn, S1, S2, ..., Sm)R
     */
    public static MethodHandle compose(MethodHandle f, MethodHandle g)
    {
        if (f.type().parameterType(0) != g.type().returnType()) {
            throw new IllegalArgumentException(format("f.parameter(0) != g.return(). f: %s  g: %s", f.type(), g.type()));
        }
        // Semantics: f => f
        // Type: (U, S1, S2, ..., Sn)R => (U, T1, T2, ..., Tm, S1, S2, ..., Sn)R
        MethodHandle fUTS = MethodHandles.dropArguments(f, 1, g.type().parameterList());
        // Semantics: f => fg
        // Type: (U, T1, T2, ..., Tm, S1, S2, ..., Sn)R => (T1, T2, ..., Tm, S1, S2, ..., Sn)R
        return MethodHandles.foldArguments(fUTS, g);
    }

    /**
     * @param f (U, V)R
     * @param g (S1, S2, ..., Sm)U
     * @param h (T1, T2, ..., Tn)V
     * @return (S1, S2, ..., Sm, T1, T2, ..., Tn)R
     */
    public static MethodHandle compose(MethodHandle f, MethodHandle g, MethodHandle h)
    {
        if (f.type().parameterCount() != 2) {
            throw new IllegalArgumentException(format("f.parameterCount != 2. f: %s", f.type()));
        }
        if (f.type().parameterType(0) != g.type().returnType()) {
            throw new IllegalArgumentException(format("f.parameter(0) != g.return. f: %s  g: %s", f.type(), g.type()));
        }
        if (f.type().parameterType(1) != h.type().returnType()) {
            throw new IllegalArgumentException(format("f.parameter(0) != h.return. f: %s  h: %s", f.type(), h.type()));
        }

        // (V, T1, T2, ..., Tn, U)R
        MethodType typeVTU = f.type().dropParameterTypes(0, 1).appendParameterTypes(h.type().parameterList()).appendParameterTypes(f.type().parameterType(0));
        // Semantics: f => f
        // Type: (U, V)R => (V, T1, T2, ..., Tn, U)R
        MethodHandle fVTU = MethodHandles.permuteArguments(f, typeVTU, h.type().parameterCount() + 1, 0);
        // Semantics: f => fh
        // Type: (V, T1, T2, ..., Tn, U)R => (T1, T2, ..., Tn, U)R
        MethodHandle fhTU = MethodHandles.foldArguments(fVTU, h);

        // reorder: [m+1, m+2, ..., m+n, 0]
        int[] reorder = new int[fhTU.type().parameterCount()];
        for (int i = 0; i < reorder.length - 1; i++) {
            reorder[i] = i + 1 + g.type().parameterCount();
        }
        reorder[reorder.length - 1] = 0;

        // (U, S1, S2, ..., Sm, T1, T2, ..., Tn)R
        MethodType typeUST = f.type().dropParameterTypes(1, 2).appendParameterTypes(g.type().parameterList()).appendParameterTypes(h.type().parameterList());
        // Semantics: f.h => f.h
        // Type: (T1, T2, ..., Tn, U)R => (U, S1, S2, ..., Sm, T1, T2, ..., Tn)R
        MethodHandle fhUST = MethodHandles.permuteArguments(fhTU, typeUST, reorder);

        // Semantics: fh => fgh
        // Type: (U, S1, S2, ..., Sm, T1, T2, ..., Tn)R => (S1, S2, ..., Sm, T1, T2, ..., Tn)R
        return MethodHandles.foldArguments(fhUST, g);
    }

    /**
     * Returns a MethodHandle corresponding to the specified method.
     * <p>
     * Warning: The way Oracle JVM implements producing MethodHandle for a method involves creating
     * JNI global weak references. G1 processes such references serially. As a result, calling this
     * method in a tight loop can create significant GC pressure and significantly increase
     * application pause time.
     */
    public static MethodHandle methodHandle(Class<?> clazz, String name, Class<?>... parameterTypes)
    {
        try {
            return MethodHandles.lookup().unreflect(clazz.getMethod(name, parameterTypes));
        }
        catch (IllegalAccessException | NoSuchMethodException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    public static MethodHandle nativeValueGetter(Type type)
    {
        Class<?> javaType = type.getJavaType();

        MethodHandle methodHandle;
        if (javaType == long.class) {
            methodHandle = GET_LONG;
        }
        else if (javaType == double.class) {
            methodHandle = GET_DOUBLE;
        }
        else if (javaType == boolean.class) {
            methodHandle = GET_BOOLEAN;
        }
        else if (javaType == Slice.class) {
            methodHandle = GET_SLICE;
        }
        else if (javaType == Block.class) {
            methodHandle = GET_BLOCK;
        }
        else {
            throw new IllegalArgumentException("Unknown java type " + javaType + " from type " + type);
        }

        return methodHandle.bindTo(type);
    }

    public static MethodHandle nativeValueWriter(Type type)
    {
        Class<?> javaType = type.getJavaType();

        MethodHandle methodHandle;
        if (javaType == long.class) {
            methodHandle = WRITE_LONG;
        }
        else if (javaType == double.class) {
            methodHandle = WRITE_DOUBLE;
        }
        else if (javaType == boolean.class) {
            methodHandle = WRITE_BOOLEAN;
        }
        else if (javaType == Slice.class) {
            methodHandle = WRITE_SLICE;
        }
        else if (javaType == Block.class) {
            methodHandle = WRITE_BLOCK;
        }
        else {
            throw new IllegalArgumentException("Unknown java type " + javaType + " from type " + type);
        }

        return methodHandle.bindTo(type);
    }
}
