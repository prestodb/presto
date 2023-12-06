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

import com.facebook.presto.spi.eventlistener.QueryContext;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.QueryMetadata;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;
import com.facebook.presto.spi.eventlistener.SplitStatistics;
import com.facebook.presto.spi.session.ResourceEstimates;
import io.ahana.eventplugin.QueryEventListener;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;

import static java.time.Duration.ofMillis;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class QueryEventListenerTest
{
    @Test
    public void queryCreatedEvents()
            throws IOException
    {
        Files.deleteIfExists(Paths.get("target/queryCreatedEvents.log"));

        try (LoggerContext loggerContext = Configurator.initialize(
                "queryCreatedEvents",
                "classpath:queryCreatedEvents.properties")) {
            // Given there is a listener for query created event
            QueryEventListener listener = new QueryEventListener(
                    "test1",
                    loggerContext,
                    false,
                    "nothing",
                    true,
                    false,
                    false);

            // When two events are created
            listener.queryCreated(prepareQueryCreatedEvent());
            listener.queryCreated(prepareQueryCreatedEvent());

            // Then two events should be present in the log file
            long logEventsCount = Files.lines(Paths.get("target/queryCreatedEvents.log")).count();
            assertEquals(2, logEventsCount);
        }
    }

    @Test
    public void onlyQueryCreatedEvents()
            throws IOException
    {
        Files.deleteIfExists(Paths.get("target/onlyQueryCreatedEvents.log"));

        try (LoggerContext loggerContext = Configurator.initialize(
                "onlyQueryCreatedEvents",
                "classpath:onlyQueryCreatedEvents.properties")) {
            // Given there is a listener for query created event
            QueryEventListener listener = new QueryEventListener(
                    "test1",
                    loggerContext,
                    false,
                    "nothing",
                    true,
                    false,
                    false);

            // When one created event is created
            //  And one split completed event is created
            listener.queryCreated(prepareQueryCreatedEvent());
            listener.splitCompleted(prepareSplitCompletedEvent());

            // Then only created event should be present in the log file
            long logEventsCount = Files.lines(Paths.get("target/onlyQueryCreatedEvents.log")).count();
            assertEquals(1, logEventsCount);
        }
    }

    private QueryCreatedEvent prepareQueryCreatedEvent()
    {
        return new QueryCreatedEvent(
                Instant.now(),
                prepareQueryContext(),
                prepareQueryMetadata());
    }

    private SplitCompletedEvent prepareSplitCompletedEvent()
    {
        return new SplitCompletedEvent(
                "queryId",
                "stageId",
                "stageExecId",
                "taskId",
                Instant.now(),
                Optional.of(Instant.now()),
                Optional.of(Instant.now()),
                getSplitStatistics(),
                Optional.empty(),
                "payload");
    }

    private SplitStatistics getSplitStatistics()
    {
        return new SplitStatistics(
                ofMillis(1000),
                ofMillis(2000),
                ofMillis(3000),
                ofMillis(4000),
                1,
                2,
                Optional.of(ofMillis(100)),
                Optional.of(ofMillis(200)));
    }

    private QueryMetadata prepareQueryMetadata()
    {
        return new QueryMetadata(
                "queryId", Optional.empty(),
                "query",
                "queryhash",
                Optional.of("query"),
                "queryState",
                URI.create("http://localhost"),
                Optional.of(""),
                Optional.of("plan"),
                Optional.of(""),
                new ArrayList<>(),
                Optional.empty());
    }

    private QueryContext prepareQueryContext()
    {
        return new QueryContext(
                "user",
                Optional.of("principal"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                new HashSet<>(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                new HashMap<>(),
                new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
                "serverAddress", "serverVersion", "environment");
    }
}
