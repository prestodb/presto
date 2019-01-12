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
package io.prestosql.testing;

import com.google.common.collect.ImmutableMap;
import io.prestosql.eventlistener.EventListenerManager;
import io.prestosql.spi.eventlistener.EventListener;
import io.prestosql.spi.eventlistener.EventListenerFactory;
import io.prestosql.spi.eventlistener.QueryCompletedEvent;
import io.prestosql.spi.eventlistener.QueryCreatedEvent;
import io.prestosql.spi.eventlistener.SplitCompletedEvent;

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
