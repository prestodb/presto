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
package com.facebook.presto.event;

import com.facebook.presto.event.query.QueryCompletionEvent;
import com.facebook.presto.event.query.QueryCreatedEvent;
import com.facebook.presto.event.query.QueryEvent;
import com.facebook.presto.event.query.QueryEventHandler;
import com.facebook.presto.event.query.SplitCompletionEvent;
import com.google.inject.Inject;
import io.airlift.event.client.AbstractEventClient;
import io.airlift.event.client.EventType;
import io.airlift.log.Logger;

import java.io.IOException;
import java.util.Set;

/**
 * Class that listens for airlift events and sends presto events to handlers
 */
public class EventProcessor extends AbstractEventClient
{
    private static final String QUERY_CREATED = "QueryCreated";
    private static final String QUERY_COMPLETION = "QueryCompletion";
    private static final String SPLIT_COMPLETION = "SplitCompletion";
    private static final Logger log = Logger.get(EventProcessor.class);

    private Set<QueryEventHandler<QueryCreatedEvent>> queryCreatedEventHandlers;
    private Set<QueryEventHandler<QueryCompletionEvent>> queryCompletionEventHandlers;
    private Set<QueryEventHandler<SplitCompletionEvent>> splitCompletionEventHandlers;

    @Inject
    public EventProcessor(
            Set<QueryEventHandler<QueryCreatedEvent>> queryCreatedEventHandlers,
            Set<QueryEventHandler<QueryCompletionEvent>> queryCompletionEventHandlers,
            Set<QueryEventHandler<SplitCompletionEvent>> splitCompletionEventHandlers)
    {
        this.queryCreatedEventHandlers = queryCreatedEventHandlers;
        this.queryCompletionEventHandlers = queryCompletionEventHandlers;
        this.splitCompletionEventHandlers = splitCompletionEventHandlers;
    }

    @Override
    protected <T> void postEvent(T event)
            throws IOException
    {
        EventType eventTypeAnnotation = event.getClass().getAnnotation(EventType.class);
        if (eventTypeAnnotation == null) {
            return;
        }

        String type = eventTypeAnnotation.value();

        switch (type) {
            case QUERY_CREATED:
                handle(queryCreatedEventHandlers, type, (QueryCreatedEvent) event);
                break;
            case QUERY_COMPLETION:
                handle(queryCompletionEventHandlers, type, (QueryCompletionEvent) event);
                break;
            case SPLIT_COMPLETION:
                handle(splitCompletionEventHandlers, type, (SplitCompletionEvent) event);
                break;
            default:
                log.warn("Unrecognized event found: " + type);
        }
    }

    private <E extends QueryEvent> void handle(Set<QueryEventHandler<E>> handlers, String type, E event)
    {
        for (QueryEventHandler<E> handler : handlers) {
            try {
                handler.handle(event);
            }
            catch (Throwable e) {
                log.error(e, String.format(
                        "Exception processing %s event for query %s", type, event.getQueryId()));
            }
        }
    }
}
