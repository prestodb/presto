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
package com.facebook.presto.testing;

import com.facebook.presto.eventlistener.EventListenerManager;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;
import com.google.common.collect.ImmutableMap;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class TestingEventListenerManager
    extends EventListenerManager
{
    private final AtomicReference<Optional<EventListener>> configuredEventListener = new AtomicReference<>(Optional.empty());

    @Override
    public void addEventListenerFactory(EventListenerFactory eventListenerFactory)
    {
        configuredEventListener.set(Optional.of(eventListenerFactory.create(ImmutableMap.of())));
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        if (configuredEventListener.get().isPresent()) {
            configuredEventListener.get().get().queryCompleted(queryCompletedEvent);
        }
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        if (configuredEventListener.get().isPresent()) {
            configuredEventListener.get().get().queryCreated(queryCreatedEvent);
        }
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
        if (configuredEventListener.get().isPresent()) {
            configuredEventListener.get().get().splitCompleted(splitCompletedEvent);
        }
    }
}
