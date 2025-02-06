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
package com.facebook.presto.common;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.predicate.Primitives;
import com.facebook.presto.common.type.Type;

import javax.annotation.Nullable;

import java.util.function.Supplier;

import static com.facebook.presto.common.type.TypeUtils.readNativeValue;
import static com.facebook.presto.common.type.TypeUtils.writeNativeValue;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class Utils
{
    private Utils()
    {
    }

    public static Block nativeValueToBlock(Type type, Object object)
    {
        if (object != null && !Primitives.wrap(type.getJavaType()).isInstance(object)) {
            throw new IllegalArgumentException(format("Object '%s' does not match type %s", object, type.getJavaType()));
        }
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
        writeNativeValue(type, blockBuilder, object);
        return blockBuilder.build();
    }

    public static Object blockToNativeValue(Type type, Block block)
    {
        return readNativeValue(type, block, 0);
    }

    public static void checkArgument(boolean expression)
    {
        if (!expression) {
            throw new IllegalArgumentException();
        }
    }

    public static void checkArgument(boolean expression, String message, Object... args)
    {
        if (!expression) {
            throw new IllegalArgumentException(format(message, args));
        }
    }

    /**
     * Returns a supplier which caches the instance retrieved during the first call to {@code get()}
     * and returns that value on subsequent calls to {@code get()}.
     */
    public static <T> Supplier<T> memoizedSupplier(Supplier<T> delegate)
    {
        if (delegate instanceof MemoizingSupplier) {
            return delegate;
        }
        return new MemoizingSupplier<>(delegate);
    }

    /**
     * Vendored from Guava
     */
    static class MemoizingSupplier<T>
            implements Supplier<T>
    {
        volatile Supplier<T> delegate;
        volatile boolean initialized;
        // "value" does not need to be volatile; visibility piggy-backs
        // on volatile read of "initialized".
        @Nullable T value;

        MemoizingSupplier(Supplier<T> delegate)
        {
            this.delegate = requireNonNull(delegate);
        }

        @Override
        public T get()
        {
            // A 2-field variant of Double Checked Locking.
            if (!initialized) {
                synchronized (this) {
                    if (!initialized) {
                        T t = delegate.get();
                        value = t;
                        initialized = true;
                        // Release the delegate to GC.
                        delegate = null;
                        return t;
                    }
                }
            }
            return value;
        }

        @Override
        public String toString()
        {
            Supplier<T> delegate = this.delegate;
            return "Suppliers.memoize("
                    + (delegate == null ? "<supplier that returned " + value + ">" : delegate)
                    + ")";
        }
    }

    public static void checkState(boolean test, String errorMessage)
    {
        if (!test) {
            throw new IllegalStateException(errorMessage);
        }
    }
}
