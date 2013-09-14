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

import com.google.common.base.Throwables;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkNotNull;

public final class MoreFutures
{
    private MoreFutures()
    {
    }

    public static <T> T tryGetUnchecked(Future<T> future)
    {
        checkNotNull(future, "future is null");
        if (!future.isDone()) {
            return null;
        }

        try {
            return future.get(0, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause == null) {
                cause = e;
            }
            throw Throwables.propagate(cause);
        }
        catch (TimeoutException e) {
            // this mean that isDone() does not agree with get()
            // this should not happen for reasonable implementations of Future
            throw Throwables.propagate(e);
        }
    }
}
