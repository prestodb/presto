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
package com.facebook.presto.operator.index;

import com.facebook.airlift.concurrent.MoreFutures;
import com.facebook.presto.common.Page;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.concurrent.ThreadSafe;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThreadSafe
class UpdateRequest
{
    private final SettableFuture<IndexSnapshot> indexSnapshotFuture = SettableFuture.create();
    private final Page page;

    public UpdateRequest(Page page)
    {
        this.page = requireNonNull(page, "page is null");
    }

    public Page getPage()
    {
        return page;
    }

    public void finished(IndexSnapshot indexSnapshot)
    {
        requireNonNull(indexSnapshot, "indexSnapshot is null");
        checkState(indexSnapshotFuture.set(indexSnapshot), "Already finished!");
    }

    public void failed(Throwable throwable)
    {
        indexSnapshotFuture.setException(throwable);
    }

    public boolean isFinished()
    {
        return indexSnapshotFuture.isDone();
    }

    public IndexSnapshot getFinishedIndexSnapshot()
    {
        checkState(indexSnapshotFuture.isDone(), "Update request is not finished");
        return MoreFutures.getFutureValue(indexSnapshotFuture);
    }
}
