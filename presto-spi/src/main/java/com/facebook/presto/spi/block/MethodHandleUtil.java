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

package com.facebook.presto.spi.block;

import com.facebook.presto.spi.PrestoException;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public final class MethodHandleUtil
{
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
            throw new IllegalArgumentException(String.format("f.parameter(0) != g.return(). f: %s  g: %s", f.type(), g.type()));
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
            throw new IllegalArgumentException(String.format("f.parameterCount != 2. f: %s", f.type()));
        }
        if (f.type().parameterType(0) != g.type().returnType()) {
            throw new IllegalArgumentException(String.format("f.parameter(0) != g.return. f: %s  g: %s", f.type(), g.type()));
        }
        if (f.type().parameterType(1) != h.type().returnType()) {
            throw new IllegalArgumentException(String.format("f.parameter(0) != h.return. f: %s  h: %s", f.type(), h.type()));
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

    public static MethodHandle methodHandle(Class<?> clazz, String name, Class<?>... parameterTypes)
    {
        try {
            return MethodHandles.lookup().unreflect(clazz.getMethod(name, parameterTypes));
        }
        catch (IllegalAccessException | NoSuchMethodException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }
}
