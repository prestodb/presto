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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static java.util.Objects.requireNonNull;

@ThreadSafe
final class SpilledLookupSourceHandle
{
    private enum State
    {
        SPILLED,
        UNSPILLING,
        PRODUCED,
        DISPOSED
    }

    @GuardedBy("this")
    private State state = State.SPILLED;

    private final SettableFuture<?> unspillingRequested = SettableFuture.create();

    @GuardedBy("this")
    @Nullable
    private SettableFuture<Supplier<LookupSource>> unspilledLookupSource;

    private final SettableFuture<?> disposeRequested = SettableFuture.create();

    private final ListenableFuture<?> unspillingOrDisposeRequested = whenAnyComplete(ImmutableList.of(unspillingRequested, disposeRequested));

    public SettableFuture<?> getUnspillingRequested()
    {
        return unspillingRequested;
    }

    public synchronized ListenableFuture<Supplier<LookupSource>> getLookupSource()
    {
        assertState(State.SPILLED);
        unspillingRequested.set(null);
        setState(State.UNSPILLING);
        checkState(unspilledLookupSource == null, "unspilledLookupSource already set");
        unspilledLookupSource = SettableFuture.create();
        return unspilledLookupSource;
    }

    public synchronized void setLookupSource(Supplier<LookupSource> lookupSource)
    {
        requireNonNull(lookupSource, "lookupSource is null");

        if (state == State.DISPOSED) {
            return;
        }

        assertState(State.UNSPILLING);
        checkState(unspilledLookupSource != null, "unspilledLookupSource not set");
        unspilledLookupSource.set(lookupSource);
        unspilledLookupSource = null; // let the memory go
        setState(State.PRODUCED);
    }

    public synchronized void dispose()
    {
        disposeRequested.set(null);
        unspilledLookupSource = null; // let the memory go
        setState(State.DISPOSED);
    }

    public SettableFuture<?> getDisposeRequested()
    {
        return disposeRequested;
    }

    public ListenableFuture<?> getUnspillingOrDisposeRequested()
    {
        return unspillingOrDisposeRequested;
    }

    @GuardedBy("this")
    private void assertState(State expectedState)
    {
        State currentState = state;
        checkState(currentState == expectedState, "Expected state %s, but state is %s", expectedState, currentState);
    }

    @GuardedBy("this")
    private void setState(State newState)
    {
        //this.state.set(requireNonNull(newState, "newState is null"));
        this.state = requireNonNull(newState, "newState is null");
    }
}
