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

import com.facebook.presto.spi.type.Type;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static java.util.Objects.requireNonNull;

public class SetSupplier
        implements SetBridge
{
    private final Type type;
    private final SettableFuture<ChannelSet> channelSetFuture = SettableFuture.create();
    private final SettableFuture<?> setNoLongerNeeded = SettableFuture.create();

    public SetSupplier(Type type)
    {
        this.type = requireNonNull(type, "type is null");
    }

    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public ListenableFuture<ChannelSet> getChannelSet()
    {
        return channelSetFuture;
    }

    @Override
    public void setChannelSet(ChannelSet channelSet)
    {
        requireNonNull(channelSet, "channelSet is null");
        boolean wasSet = channelSetFuture.set(requireNonNull(channelSet, "channelSet is null"));
        checkState(wasSet, "ChannelSet already set");
    }

    @Override
    public ListenableFuture<?> isDestroyed()
    {
        return nonCancellationPropagating(setNoLongerNeeded);
    }

    @Override
    public void destroy()
    {
        // Let the SetBuildOperator declare that it's finished.
        setNoLongerNeeded.set(null);
    }
}
