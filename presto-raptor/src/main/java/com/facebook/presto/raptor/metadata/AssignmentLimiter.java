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
package com.facebook.presto.raptor.metadata;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.PrestoException;
import com.facebook.presto.raptor.NodeSupplier;
import com.facebook.presto.spi.Node;
import com.google.common.base.Ticker;
import io.airlift.units.Duration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_NOT_ENOUGH_NODES;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_REASSIGNMENT_DELAY;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_REASSIGNMENT_THROTTLE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;

public class AssignmentLimiter
{
    private static final Logger log = Logger.get(AssignmentLimiter.class);

    private final NodeSupplier nodeSupplier;
    private final Ticker ticker;
    private final Duration reassignmentDelay;
    private final Duration reassignmentInterval;
    private final int minimumNodeCount;

    private final ScheduledExecutorService scheduler = newScheduledThreadPool(1, daemonThreadsNamed("assignment-limiter"));
    private final AtomicBoolean started = new AtomicBoolean();

    @GuardedBy("this")
    private final Map<String, Long> delayedNodes = new HashMap<>();
    @GuardedBy("this")
    private final Set<String> offlineNodes = new HashSet<>();
    @GuardedBy("this")
    private OptionalLong lastOfflined = OptionalLong.empty();

    @Inject
    public AssignmentLimiter(NodeSupplier nodeSupplier, Ticker ticker, MetadataConfig config)
    {
        this(nodeSupplier, ticker, config.getReassignmentDelay(), config.getReassignmentInterval(), config.getMinimumNodeCount());
    }

    public AssignmentLimiter(NodeSupplier nodeSupplier, Ticker ticker, Duration reassignmentDelay, Duration reassignmentInterval, int minimumNodeCount)
    {
        this.nodeSupplier = requireNonNull(nodeSupplier, "nodeSupplier is null");
        this.ticker = requireNonNull(ticker, "ticker is null");
        this.reassignmentDelay = requireNonNull(reassignmentDelay, "reassignmentDelay is null");
        this.reassignmentInterval = requireNonNull(reassignmentInterval, "reassignmentInterval is null");
        this.minimumNodeCount = minimumNodeCount;
    }

    @PostConstruct
    public void start()
    {
        if (!started.getAndSet(true)) {
            scheduler.scheduleWithFixedDelay(() -> {
                try {
                    clearOnlineNodes();
                }
                catch (Throwable t) {
                    log.error(t, "Error clearing online nodes");
                }
            }, 2, 2, SECONDS);
        }
    }

    @PreDestroy
    public void shutdown()
    {
        scheduler.shutdownNow();
    }

    public synchronized void checkAssignFrom(String nodeIdentifier)
    {
        int currentNodeCount = nodeSupplier.getWorkerNodes().size();
        if (currentNodeCount < minimumNodeCount) {
            throw new PrestoException(RAPTOR_NOT_ENOUGH_NODES, format("Not enough nodes available (required: %s, current: %s)", minimumNodeCount, currentNodeCount));
        }

        if (offlineNodes.contains(nodeIdentifier)) {
            return;
        }

        long now = ticker.read();
        long start = delayedNodes.computeIfAbsent(nodeIdentifier, key -> now);
        Duration delay = new Duration(now - start, NANOSECONDS);

        if (delay.compareTo(reassignmentDelay) < 0) {
            throw new PrestoException(RAPTOR_REASSIGNMENT_DELAY, format(
                    "Reassignment delay is in effect for node %s (elapsed: %s)",
                    nodeIdentifier,
                    delay.convertToMostSuccinctTimeUnit()));
        }

        if (lastOfflined.isPresent()) {
            delay = new Duration(now - lastOfflined.getAsLong(), NANOSECONDS);
            if (delay.compareTo(reassignmentInterval) < 0) {
                throw new PrestoException(RAPTOR_REASSIGNMENT_THROTTLE, format(
                        "Reassignment throttle is in effect for node %s (elapsed: %s)",
                        nodeIdentifier,
                        delay.convertToMostSuccinctTimeUnit()));
            }
        }

        delayedNodes.remove(nodeIdentifier);
        offlineNodes.add(nodeIdentifier);
        lastOfflined = OptionalLong.of(now);
    }

    private void clearOnlineNodes()
    {
        Set<String> onlineNodes = nodeSupplier.getWorkerNodes().stream()
                .map(Node::getNodeIdentifier)
                .collect(toSet());

        synchronized (this) {
            delayedNodes.keySet().removeAll(onlineNodes);
            offlineNodes.removeAll(onlineNodes);
        }
    }
}
