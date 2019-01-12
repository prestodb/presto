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
package io.prestosql.plugin.raptor.legacy.util;

import static java.util.Objects.requireNonNull;

public class Closer<T, X extends Exception>
        implements AutoCloseable
{
    private final T delegate;
    private final Cleaner<T, X> cleaner;

    public static <T, X extends Exception> Closer<T, X> closer(T delegate, Cleaner<T, X> cleaner)
    {
        return new Closer<>(delegate, cleaner);
    }

    private Closer(T delegate, Cleaner<T, X> cleaner)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.cleaner = requireNonNull(cleaner, "cleaner is null");
    }

    public T get()
    {
        return delegate;
    }

    @Override
    public void close()
            throws X
    {
        cleaner.close(delegate);
    }

    public interface Cleaner<T, X extends Exception>
    {
        void close(T object)
                throws X;
    }
}
