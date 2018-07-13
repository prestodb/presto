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

import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

public class SharedSetBridge
        implements SetBridge
{
    private final SetBridge delegate;
    private final Runnable onDestroy;

    private final AtomicBoolean destroyed = new AtomicBoolean(false);

    public SharedSetBridge(SetBridge delegate, Runnable onDestroy)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.onDestroy = requireNonNull(onDestroy, "onDestroy is null");
    }

    @Override
    public Type getType()
    {
        return delegate.getType();
    }

    @Override
    public ListenableFuture<ChannelSet> getChannelSet()
    {
        return delegate.getChannelSet();
    }

    @Override
    public void setChannelSet(ChannelSet channelSet)
    {
        delegate.setChannelSet(channelSet);
    }

    @Override
    public ListenableFuture<?> isDestroyed()
    {
        return delegate.isDestroyed();
    }

    @Override
    public void destroy()
    {
        if (destroyed.compareAndSet(false, true)) {
            onDestroy.run();
        }
    }
}
