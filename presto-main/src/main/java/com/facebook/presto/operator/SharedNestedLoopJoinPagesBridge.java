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

import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

@ThreadSafe
public final class SharedNestedLoopJoinPagesBridge
        implements NestedLoopJoinPagesBridge
{
    private final NestedLoopJoinPagesBridge delegate;
    private final Runnable onDestroy;

    private final AtomicBoolean destroyed = new AtomicBoolean(false);

    public SharedNestedLoopJoinPagesBridge(NestedLoopJoinPagesBridge delegate, Runnable onDestroy)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.onDestroy = requireNonNull(onDestroy, "onDestroy is null");
    }

    @Override
    public ListenableFuture<NestedLoopJoinPages> getPagesFuture()
    {
        return delegate.getPagesFuture();
    }

    @Override
    public ListenableFuture<?> setPages(NestedLoopJoinPages nestedLoopJoinPages)
    {
        return delegate.setPages(nestedLoopJoinPages);
    }

    @Override
    public void destroy()
    {
        if (destroyed.compareAndSet(false, true)) {
            onDestroy.run();
        }
    }
}
