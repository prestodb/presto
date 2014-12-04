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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class UnnestOperator
        implements Operator
{
    public static class UnnestOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final List<Integer> replicateChannels;
        private final List<Type> replicateTypes;
        private final List<Integer> unnestChannels;
        private final List<Type> unnestTypes;
        private boolean closed;
        private final ImmutableList<Type> types;

        public UnnestOperatorFactory(int operatorId, List<Integer> replicateChannels, List<Type> replicateTypes, List<Integer> unnestChannels, List<Type> unnestTypes)
        {
            this.operatorId = operatorId;
            this.replicateChannels = ImmutableList.copyOf(checkNotNull(replicateChannels, "replicateChannels is null"));
            this.replicateTypes = ImmutableList.copyOf(checkNotNull(replicateTypes, "replicateTypes is null"));
            checkArgument(replicateChannels.size() == replicateTypes.size(), "replicateChannels and replicateTypes do not match");
            this.unnestChannels = ImmutableList.copyOf(checkNotNull(unnestChannels, "unnestChannels is null"));
            this.unnestTypes = ImmutableList.copyOf(checkNotNull(unnestTypes, "unnestTypes is null"));
            checkArgument(unnestChannels.size() == unnestTypes.size(), "unnestChannels and unnestTypes do not match");
            types = ImmutableList.<Type>builder()
                    .addAll(replicateTypes)
                    .addAll(getUnnestedTypes(unnestTypes))
                    .build();
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, UnnestOperator.class.getSimpleName());
            return new UnnestOperator(operatorContext, replicateChannels, replicateTypes, unnestChannels, unnestTypes);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final List<Integer> replicateChannels;
    private final List<Type> replicateTypes;
    private final List<Integer> unnestChannels;
    private final List<Type> unnestTypes;
    private final List<Type> outputTypes;
    private final PageBuilder pageBuilder;
    private final List<Unnester> unnesters;
    private boolean finishing;
    private Page currentPage;
    private int currentPosition;

    public UnnestOperator(OperatorContext operatorContext, List<Integer> replicateChannels, List<Type> replicateTypes, List<Integer> unnestChannels, List<Type> unnestTypes)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.replicateChannels = ImmutableList.copyOf(checkNotNull(replicateChannels, "replicateChannels is null"));
        this.replicateTypes = ImmutableList.copyOf(checkNotNull(replicateTypes, "replicateTypes is null"));
        this.unnestChannels = ImmutableList.copyOf(checkNotNull(unnestChannels, "unnestChannels is null"));
        this.unnestTypes = ImmutableList.copyOf(checkNotNull(unnestTypes, "unnestTypes is null"));
        checkArgument(replicateChannels.size() == replicateTypes.size(), "replicate channels or types has wrong size");
        checkArgument(unnestChannels.size() == unnestTypes.size(), "unnest channels or types has wrong size");
        outputTypes = ImmutableList.<Type>builder()
                .addAll(replicateTypes)
                .addAll(getUnnestedTypes(unnestTypes))
                .build();
        this.pageBuilder = new PageBuilder(outputTypes);
        this.unnesters = new ArrayList<>();
    }

    private static List<Type> getUnnestedTypes(List<Type> types)
    {
        ImmutableList.Builder<Type> builder = ImmutableList.builder();
        for (Type type : types) {
            checkArgument(type instanceof ArrayType || type instanceof MapType, "Can only unnest map and array types");
            builder.addAll(type.getTypeParameters());
        }
        return builder.build();
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public final List<Type> getTypes()
    {
        return outputTypes;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finishing && pageBuilder.isEmpty() && currentPage == null;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && !pageBuilder.isFull() && currentPage == null;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        checkNotNull(page, "page is null");
        checkState(currentPage == null, "currentPage is not null");
        checkState(!pageBuilder.isFull(), "Page buffer is full");

        currentPage = page;
        currentPosition = 0;
        initializeUnnesters();
    }

    private void initializeUnnesters()
    {
        unnesters.clear();
        for (int i = 0; i < unnestTypes.size(); i++) {
            Type type = unnestTypes.get(i);
            int channel = unnestChannels.get(i);
            Slice slice = null;
            if (!currentPage.getBlock(channel).isNull(currentPosition)) {
                slice = type.getSlice(currentPage.getBlock(channel), currentPosition);
            }
            if (type instanceof ArrayType) {
                unnesters.add(new ArrayUnnester((ArrayType) type, slice));
            }
            else if (type instanceof MapType) {
                unnesters.add(new MapUnnester((MapType) type, slice));
            }
            else {
                throw new IllegalArgumentException("Cannot unnest type: " + type);
            }
        }
    }

    private boolean anyUnnesterHasData()
    {
        for (Unnester unnester : unnesters) {
            if (unnester.hasNext()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Page getOutput()
    {
        while (!pageBuilder.isFull() && currentPage != null) {
            // Advance until we find data to unnest
            while (!anyUnnesterHasData()) {
                currentPosition++;
                if (currentPosition == currentPage.getPositionCount()) {
                    currentPage = null;
                    currentPosition = 0;
                    break;
                }
                initializeUnnesters();
            }
            while (!pageBuilder.isFull() && anyUnnesterHasData()) {
                // Copy all the channels marked for replication
                for (int replicateChannel = 0; replicateChannel < replicateTypes.size(); replicateChannel++) {
                    Type type = replicateTypes.get(replicateChannel);
                    int channel = replicateChannels.get(replicateChannel);
                    type.appendTo(currentPage.getBlock(channel), currentPosition, pageBuilder.getBlockBuilder(replicateChannel));
                }
                int offset = replicateTypes.size();

                pageBuilder.declarePosition();
                for (Unnester unnester : unnesters) {
                    if (unnester.hasNext()) {
                        unnester.appendNext(pageBuilder, offset);
                    }
                    else {
                        for (int unnesterChannelIndex = 0; unnesterChannelIndex < unnester.getChannelCount(); unnesterChannelIndex++) {
                            pageBuilder.getBlockBuilder(offset + unnesterChannelIndex).appendNull();
                        }
                    }
                    offset += unnester.getChannelCount();
                }
            }
        }

        if ((!finishing && !pageBuilder.isFull()) || pageBuilder.isEmpty()) {
            return null;
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }
}
