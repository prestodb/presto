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
package com.facebook.presto.maxcompute.util;

import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.PrestoException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public final class Types
{
    private Types()
    {
    }

    public static <A, B extends A> B checkType(A value, Class<B> target, String name)
    {
        if (value == null) {
            throw new NullPointerException(format("%s is null", name));
        }
        checkArgument(target.isInstance(value),
                "%s must be of type %s, not %s",
                name,
                target.getName(),
                value.getClass().getName());
        return target.cast(value);
    }

    public static <A, B extends A> B checkType(A value, Class<B> target, ErrorCodeSupplier errorCode, String name)
    {
        if (value == null) {
            throw new NullPointerException(format("%s is null", name));
        }
        if (!target.isInstance(value)) {
            throw new PrestoException(errorCode, format(
                    "%s must be of type %s, not %s",
                    name,
                    target.getName(),
                    value.getClass().getName()));
        }
        return target.cast(value);
    }
}
