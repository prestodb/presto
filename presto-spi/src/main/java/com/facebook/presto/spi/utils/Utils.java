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
package com.facebook.presto.spi.utils;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class Utils
{
    private Utils() {}

    public static <T> List<T> immutableListCopyOf(List<T> list)
    {
        return unmodifiableList(new ArrayList<>(requireNonNull(list, "list is null")));
    }

    public static void checkArgument(boolean test, String errorMessage, Object... arguments)
    {
        if (!test) {
            throw new IllegalArgumentException(format(errorMessage, arguments));
        }
    }

    public static void checkState(boolean test, String errorMessage, Object... arguments)
    {
        if (!test) {
            throw new IllegalStateException(format(errorMessage, arguments));
        }
    }

    public static void verify(boolean test, String errorMessage)
    {
        if (!test) {
            throw new AssertionError(errorMessage);
        }
    }

    public static void checkArgument(boolean test)
    {
        if (!test) {
            throw new IllegalArgumentException();
        }
    }

    public static void checkState(boolean test)
    {
        if (!test) {
            throw new IllegalStateException();
        }
    }

    public static <T> T getOnlyElement(List<T> list)
    {
        requireNonNull(list, "list is null");
        checkArgument(list.size() == 1, "expect list to have exactly one element");
        return list.get(0);
    }
}
