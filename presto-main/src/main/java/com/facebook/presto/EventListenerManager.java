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

package com.facebook.presto;

import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class EventListenerManager
{
    private final ConcurrentMap<String, EventListener> eventListeners = new ConcurrentHashMap<>();

    public void addEventListenerFactory(EventListenerFactory eventListenerFactory)
    {
        requireNonNull(eventListenerFactory, "eventListenerFactory is null");
        EventListener existingEntry = eventListeners.putIfAbsent(eventListenerFactory.getClass().getName(), eventListenerFactory.createEventListener(null));
        checkArgument(existingEntry == null, "Event listener %s is already registered", eventListenerFactory.getClass().getName());
    }

    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        for (String eventListenerClass : eventListeners.keySet()) {
            eventListeners.get(eventListenerClass).queryCompleted(queryCompletedEvent);
        }
    }

    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        for (String eventListenerClass : eventListeners.keySet()) {
            eventListeners.get(eventListenerClass).queryCreated(queryCreatedEvent);
        }
    }
}
