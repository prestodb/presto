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
package com.facebook.presto.tablestore;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class TypeUtil
{
    private TypeUtil()
    {
    }

    public static <A, B extends A> B checkType(A value, Class<B> target)
    {
        requireNonNull(value, format("We need a nonnull instance of type[%s], but it's null.", target.getSimpleName()));
        checkArgument(target.isInstance(value),
                "We need an instance of type[%s], but it's the type[%s].",
                target.getName(), value.getClass().getName());
        return target.cast(value);
    }
}
