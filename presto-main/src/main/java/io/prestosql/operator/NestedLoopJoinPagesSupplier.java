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
package io.prestosql.operator;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public final class NestedLoopJoinPagesSupplier
        implements NestedLoopJoinBridge
{
    private final SettableFuture<NestedLoopJoinPages> pagesFuture = SettableFuture.create();
    private final SettableFuture<?> pagesNoLongerNeeded = SettableFuture.create();

    @Override
    public ListenableFuture<NestedLoopJoinPages> getPagesFuture()
    {
        return transformAsync(pagesFuture, Futures::immediateFuture, directExecutor());
    }

    @Override
    public ListenableFuture<?> setPages(NestedLoopJoinPages nestedLoopJoinPages)
    {
        requireNonNull(nestedLoopJoinPages, "nestedLoopJoinPages is null");
        boolean wasSet = pagesFuture.set(nestedLoopJoinPages);
        checkState(wasSet, "pagesFuture already set");
        return pagesNoLongerNeeded;
    }

    @Override
    public void destroy()
    {
        // Let the NestedLoopBuildOperator declare that it's finished.
        pagesNoLongerNeeded.set(null);
    }
}
