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
package com.facebook.presto.operator;

import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.Objects.requireNonNull;

public class Operators
{
    private Operators() {}

    public static <T> T getDone(Future<T> future)
    {
        requireNonNull(future, "future is null");
        checkArgument(future.isDone(), "future not done yet");
        return getFutureValue(future);
    }

    /**
     * Checks that the completed future completed successfully.
     */
    public static void checkSuccess(Future<?> future, String errorMessage)
    {
        requireNonNull(future, "future is null");
        requireNonNull(errorMessage, "errorMessage is null");
        checkArgument(future.isDone(), "future not done yet");
        try {
            getFutureValue(future);
        }
        catch (RuntimeException e) {
            throw new IllegalArgumentException(errorMessage, e);
        }
    }
}
