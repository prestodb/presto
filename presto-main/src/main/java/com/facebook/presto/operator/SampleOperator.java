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
import com.google.common.collect.ImmutableList;
import org.apache.commons.math3.random.RandomDataGenerator;

import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class SampleOperator
        implements Operator
{
    public static class SampleOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final double sampleRatio;
        private final boolean rescaled;

        private final List<Type> types;
        private boolean closed;

        public SampleOperatorFactory(int operatorId, double sampleRatio, boolean rescaled, List<Type> sourceTypes)
        {
            this.operatorId = operatorId;
            this.sampleRatio = sampleRatio;
            this.rescaled = rescaled;
            this.types = ImmutableList.<Type>builder()
                    .addAll(checkNotNull(sourceTypes, "sourceTypes is null"))
                    .add(BIGINT)
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
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, SampleOperator.class.getSimpleName());
            return new SampleOperator(operatorContext, sampleRatio, rescaled, types);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final List<Type> types;
    private final PageBuilder pageBuilder;
    private final RandomDataGenerator rand = new RandomDataGenerator();
    private final double sampleRatio;
    private final boolean rescaled;
    private final int sampleWeightChannel;
    private boolean finishing;

    private int position = -1;
    private Page page;

    public SampleOperator(OperatorContext operatorContext, double sampleRatio, boolean rescaled, List<Type> types)
    {
        //Note: Poissonized Samples can be larger than the original dataset if desired
        checkArgument(sampleRatio > 0, "sample ratio must be strictly positive");

        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.types = ImmutableList.copyOf(types);
        this.pageBuilder = new PageBuilder(types);
        this.sampleWeightChannel = types.size() - 1;
        this.sampleRatio = sampleRatio;
        this.rescaled = rescaled;
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
    public final void finish()
    {
        finishing = true;
    }

    @Override
    public final boolean isFinished()
    {
        return finishing && pageBuilder.isEmpty() && page == null;
    }

    @Override
    public final boolean needsInput()
    {
        return !finishing && !pageBuilder.isFull() && page == null;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        checkNotNull(page, "page is null");
        checkState(!pageBuilder.isFull(), "Page buffer is full");
        checkState(this.page == null, "previous page has not been completely processed");

        this.page = page;
        this.position = 0;
    }

    @Override
    public Page getOutput()
    {
        if (page != null) {
            while (position < page.getPositionCount() && !pageBuilder.isFull()) {
                long repeats = rand.nextPoisson(sampleRatio);
                if (rescaled && repeats > 0) {
                    repeats *= rand.nextPoisson(1.0 / sampleRatio);
                }

                if (repeats > 0) {
                    // copy input values to output page
                    // NOTE: last output type is sample weight so we skip it
                    pageBuilder.declarePosition();
                    for (int channel = 0; channel < types.size() - 1; channel++) {
                        Type type = types.get(channel);
                        type.appendTo(page.getBlock(channel), position, pageBuilder.getBlockBuilder(channel));
                    }
                    BIGINT.writeLong(pageBuilder.getBlockBuilder(sampleWeightChannel), repeats);
                }

                position++;
            }

            if (position >= page.getPositionCount()) {
                page = null;
            }
        }

        // only flush full pages unless we are done
        if (pageBuilder.isFull() || (finishing && !pageBuilder.isEmpty())) {
            Page page = pageBuilder.build();
            pageBuilder.reset();
            return page;
        }

        return null;
    }
}
