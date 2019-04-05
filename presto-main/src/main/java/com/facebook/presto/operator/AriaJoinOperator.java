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

import com.facebook.presto.operator.LookupJoinOperators.JoinType;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.concurrent.MoreFutures.getDone;
import static java.util.Objects.requireNonNull;

public class AriaJoinOperator
        implements Operator
{
    private final OperatorContext operatorContext;
    private final JoinProbe.JoinProbeFactory joinProbeFactory;
    private final Runnable afterClose;
    private final OptionalInt lookupJoinsCount;
    private final LookupSourceFactory lookupSourceFactory;
    private final LookupJoinPageBuilder pageBuilder;
    private final ListenableFuture<LookupSourceProvider> lookupSourceProviderFuture;

    private LookupSourceProvider lookupSourceProvider;
    private JoinProbe probe;
    private AriaProbe ariaProbe;

    private Page outputPage;

    private boolean closed;
    private boolean finishing;
    private boolean finished;

    @Nullable
    private ListenableFuture<PartitionedConsumption<Supplier<LookupSource>>> partitionedConsumption;
    @Nullable
    private boolean reusePages;

    public AriaJoinOperator(
            OperatorContext operatorContext,
            List<Type> probeTypes,
            List<Type> buildOutputTypes,
            JoinType joinType,
            LookupSourceFactory lookupSourceFactory,
            JoinProbe.JoinProbeFactory joinProbeFactory,
            Runnable afterClose,
            OptionalInt lookupJoinsCount,
            HashGenerator hashGenerator)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        requireNonNull(joinType, "joinType is null");

        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");
        this.afterClose = requireNonNull(afterClose, "afterClose is null");
        this.lookupJoinsCount = requireNonNull(lookupJoinsCount, "lookupJoinsCount is null");
        this.lookupSourceFactory = requireNonNull(lookupSourceFactory, "lookupSourceFactory is null");
        this.lookupSourceProviderFuture = lookupSourceFactory.createLookupSourceProvider();

        this.pageBuilder = new LookupJoinPageBuilder(buildOutputTypes);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        if (finishing) {
            return;
        }
        if (ariaProbe != null) {
            ariaProbe.finish();
        }
        if (lookupSourceProvider != null) {
            lookupSourceProvider.withLease(lookupSourceLease -> {
                LookupSource lookupSource = lookupSourceLease.getLookupSource();
                lookupSource.close();
                return true;
            });
        }
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        if (finished || closed) {
            return true;
        }
        boolean finished;

        if (ariaProbe != null) {
            finished = ariaProbe.isFinished();
        }
        else {
            finished = this.finished && probe == null && pageBuilder.isEmpty() && outputPage == null;
        }
        // if finished drop references so memory is freed early
        if (finished) {
            close();
        }
        return finished;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (finishing) {
            return NOT_BLOCKED;
        }

        return lookupSourceProviderFuture;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing
                && lookupSourceProviderFuture.isDone()
                && probe == null
                && outputPage == null;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(probe == null, "Current page has not been completely processed yet");

        checkState(tryFetchLookupSourceProvider(), "Not ready to handle input yet");

        // create probe
        probe = joinProbeFactory.createJoinProbe(page);
    }

    private boolean tryFetchLookupSourceProvider()
    {
        if (lookupSourceProvider == null) {
            if (!lookupSourceProviderFuture.isDone()) {
                return false;
            }
            lookupSourceProvider = requireNonNull(getDone(lookupSourceProviderFuture));
        }
        return true;
    }

    @Override
    public Page getOutput()
    {
        if (probe == null && pageBuilder.isEmpty() && !finishing) {
            return null;
        }

        if (!tryFetchLookupSourceProvider()) {
            if (!finishing) {
                return null;
            }

            verify(finishing);
            // We are no longer interested in the build side (the lookupSourceProviderFuture's value).
            addSuccessCallback(lookupSourceProviderFuture, LookupSourceProvider::close);
            lookupSourceProvider = new StaticLookupSourceProvider(new EmptyLookupSource());
        }

        if (probe == null && finishing && partitionedConsumption == null) {
            /*
             * We do not have input probe and we won't have any, as we're finishing.
             * Let LookupSourceFactory know LookupSources can be disposed as far as we're concerned.
             */
            partitionedConsumption = lookupSourceFactory.finishProbeOperator(lookupJoinsCount);
        }

        if (probe == null && !finished) {
            finished = true;
        }

        if (probe != null) {
            processProbe();
        }

        if (outputPage != null) {
            verify(pageBuilder.isEmpty());
            Page output = outputPage;
            outputPage = null;

            return output;
        }

        // It is impossible to have probe == null && !pageBuilder.isEmpty(),
        // because we will flush a page whenever we reach the probe end
        verify(probe != null || pageBuilder.isEmpty());
        return null;
    }

    private void processProbe()
    {
        lookupSourceProvider.withLease(lookupSourceLease -> {
            LookupSource lookupSource = lookupSourceLease.getLookupSource();
            if (ariaProbe == null) {
                if (!(lookupSource instanceof PartitionedLookupSource)) {
                    return Optional.empty();
                }
                ariaProbe = getAriaProbe((PartitionedLookupSource) lookupSource);
            }
            if (probe.getPosition() == -1) {
                ariaProbe.addInput(probe);
                probe.advanceNextPosition();
            }
            outputPage = ariaProbe.getOutput();
            if (outputPage == null) {
                probe = null;
            }
            return Optional.empty();
        });
    }

    private AriaProbe getAriaProbe(LookupSource lookupSource)
    {
        return new AriaProbe(lookupSource.getPartitionToLookupSourceSupplier(), reusePages);
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        probe = null;

        try (Closer closer = Closer.create()) {
            // `afterClose` must be run last.
            // Closer is documented to mimic try-with-resource, which implies close will happen in reverse order.
            closer.register(afterClose::run);

            closer.register(pageBuilder::reset);
            closer.register(() -> Optional.ofNullable(lookupSourceProvider).ifPresent(LookupSourceProvider::close));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
