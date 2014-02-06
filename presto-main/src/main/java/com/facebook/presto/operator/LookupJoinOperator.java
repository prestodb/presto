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

import com.facebook.presto.operator.HashBuilderOperator.HashSupplier;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.facebook.presto.util.MoreFutures.tryGetUnchecked;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class LookupJoinOperator
        implements Operator
{
    private final ListenableFuture<JoinHash> hashFuture;

    private final OperatorContext operatorContext;
    private final JoinProbeFactory joinProbeFactory;
    private final boolean enableOuterJoin;
    private final List<Type> types;
    private final PageBuilder pageBuilder;

    private JoinHash hash;
    private JoinProbe probe;

    private boolean finishing;
    private int joinPosition = -1;

    public LookupJoinOperator(
            OperatorContext operatorContext,
            HashSupplier hashSupplier,
            List<Type> probeTypes,
            boolean enableOuterJoin,
            JoinProbeFactory joinProbeFactory)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");

        // todo pass in desired projection
        checkNotNull(hashSupplier, "hashSupplier is null");
        checkNotNull(probeTypes, "probeTypes is null");

        this.hashFuture = hashSupplier.getSourceHash();
        this.joinProbeFactory = joinProbeFactory;
        this.enableOuterJoin = enableOuterJoin;

        this.types = ImmutableList.<Type>builder()
                .addAll(probeTypes)
                .addAll(hashSupplier.getTypes())
                .build();
        this.pageBuilder = new PageBuilder(types);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        boolean finished = finishing && probe == null && pageBuilder.isEmpty();

        // if finished drop references so memory is freed early
        if (finished) {
            hash = null;

            probe = null;
            pageBuilder.reset();
        }
        return finished;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return hashFuture;
    }

    @Override
    public boolean needsInput()
    {
        if (finishing) {
            return false;
        }

        if (hash == null) {
            hash = tryGetUnchecked(hashFuture);
        }
        return hash != null && probe == null;
    }

    @Override
    public void addInput(Page page)
    {
        checkNotNull(page, "page is null");
        checkState(!finishing, "Operator is finishing");
        checkState(hash != null, "Hash has not been built yet");
        checkState(probe == null, "Current page has not been completely processed yet");

        // create probe
        probe = joinProbeFactory.createJoinProbe(hash, page);

        // initialize to invalid join position to force output code to advance the cursors
        joinPosition = -1;
    }

    @Override
    public Page getOutput()
    {
        // join probe page with the hash
        if (probe != null) {
            while (joinCurrentPosition()) {
                if (!advanceProbePosition()) {
                    break;
                }
                if (!outerJoinCurrentPosition()) {
                    break;
                }
            }
        }

        // only flush full pages unless we are done
        if (pageBuilder.isFull() || (finishing && !pageBuilder.isEmpty() && probe == null)) {
            Page page = pageBuilder.build();
            pageBuilder.reset();
            return page;
        }

        return null;
    }

    private boolean joinCurrentPosition()
    {
        // while we have a position to join against...
        while (joinPosition >= 0) {
            // write probe columns
            probe.appendTo(pageBuilder);

            // write build columns
            hash.appendTo(joinPosition, pageBuilder, probe.getChannelCount());

            // get next join position for this row
            joinPosition = hash.getNextJoinPosition(joinPosition);
            if (pageBuilder.isFull()) {
                return false;
            }
        }
        return true;
    }

    private boolean advanceProbePosition()
    {
        if (!probe.advanceNextPosition()) {
            probe = null;
            return false;
        }

        // update join position
        joinPosition = probe.getCurrentJoinPosition();
        return true;
    }

    private boolean outerJoinCurrentPosition()
    {
        if (enableOuterJoin && joinPosition < 0) {
            // write probe columns
            probe.appendTo(pageBuilder);

            // write nulls into build columns
            int outputIndex = probe.getChannelCount();
            for (int buildChannel = 0; buildChannel < hash.getChannelCount(); buildChannel++) {
                pageBuilder.getBlockBuilder(outputIndex).appendNull();
                outputIndex++;
            }
            if (pageBuilder.isFull()) {
                return false;
            }
        }
        return true;
    }
}
