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
package com.facebook.presto.raptor.util;

public final class Closeables
{
    private Closeables() {}

    @SuppressWarnings("ObjectEquality")
    public static void closeWithSuppression(Throwable throwable, AutoCloseable... closeables)
    {
        for (AutoCloseable closeable : closeables) {
            try {
                if (closeable != null) {
                    closeable.close();
                }
            }
            catch (Throwable t) {
                // Self-suppression not permitted
                if (t != throwable) {
                    throwable.addSuppressed(t);
                }
            }
        }
    }
}
