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
package com.facebook.presto.operator.table;

import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.table.AbstractConnectorTableFunction;
import com.facebook.presto.spi.function.table.Argument;
import com.facebook.presto.spi.function.table.ConnectorTableFunction;
import com.facebook.presto.spi.function.table.ConnectorTableFunctionHandle;
import com.facebook.presto.spi.function.table.DescribedTableReturnTypeSpecification;
import com.facebook.presto.spi.function.table.ScalarArgument;
import com.facebook.presto.spi.function.table.ScalarArgumentSpecification;
import com.facebook.presto.spi.function.table.TableFunctionAnalysis;
import com.facebook.presto.spi.function.table.TableFunctionProcessorProvider;
import com.facebook.presto.spi.function.table.TableFunctionProcessorState;
import com.facebook.presto.spi.function.table.TableFunctionSplitProcessor;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Provider;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.operator.table.Sequence.SequenceFunctionSplit.MAX_SPLIT_SIZE;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.function.table.Descriptor.descriptor;
import static com.facebook.presto.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static com.facebook.presto.spi.function.table.TableFunctionProcessorState.Processed.produced;
import static com.facebook.presto.spi.function.table.TableFunctionProcessorState.Processed.usedInputAndProduced;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class Sequence
        implements Provider<ConnectorTableFunction>
{
    public static final String NAME = "sequence";

    @Override
    public ConnectorTableFunction get()
    {
        return new SequenceFunction();
    }

    public static class SequenceFunction
            extends AbstractConnectorTableFunction
    {
        private static final String START_ARGUMENT_NAME = "START";
        private static final String STOP_ARGUMENT_NAME = "STOP";
        private static final String STEP_ARGUMENT_NAME = "STEP";

        public SequenceFunction()
        {
            super(
                    "builtin",
                    NAME,
                    ImmutableList.of(
                            ScalarArgumentSpecification.builder()
                                    .name(START_ARGUMENT_NAME)
                                    .type(BIGINT)
                                    .defaultValue(0L)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name(STOP_ARGUMENT_NAME)
                                    .type(BIGINT)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name(STEP_ARGUMENT_NAME)
                                    .type(BIGINT)
                                    .defaultValue(1L)
                                    .build()),
                    new DescribedTableReturnTypeSpecification(descriptor(ImmutableList.of("sequential_number"), ImmutableList.of(BIGINT))));
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            Object startValue = ((ScalarArgument) arguments.get(START_ARGUMENT_NAME)).getValue();
            if (startValue == null) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Start is null");
            }

            Object stopValue = ((ScalarArgument) arguments.get(STOP_ARGUMENT_NAME)).getValue();
            if (stopValue == null) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Stop is null");
            }

            Object stepValue = ((ScalarArgument) arguments.get(STEP_ARGUMENT_NAME)).getValue();
            if (stepValue == null) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Step is null");
            }

            long start = (long) startValue;
            long stop = (long) stopValue;
            long step = (long) stepValue;

            if (start < stop && step <= 0) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Step must be positive for sequence [%s, %s]", start, stop));
            }

            if (start > stop && step >= 0) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Step must be negative for sequence [%s, %s]", start, stop));
            }

            return TableFunctionAnalysis.builder()
                    .handle(new SequenceFunctionHandle(start, stop, start == stop ? 0 : step))
                    .build();
        }
    }

    public static class SequenceFunctionHandle
            implements ConnectorTableFunctionHandle
    {
        private final long start;
        private final long stop;
        private final long step;

        @JsonCreator
        public SequenceFunctionHandle(@JsonProperty("start") long start, @JsonProperty("stop") long stop, @JsonProperty("step") long step)
        {
            this.start = start;
            this.stop = stop;
            this.step = step;
        }

        @JsonProperty
        public long start()
        {
            return start;
        }

        @JsonProperty
        public long stop()
        {
            return stop;
        }

        @JsonProperty
        public long step()
        {
            return step;
        }
    }

    public static ConnectorSplitSource getSequenceFunctionSplitSource(SequenceFunctionHandle handle)
    {
        // using BigInteger to avoid long overflow since it's not in the main data processing loop
        BigInteger start = BigInteger.valueOf(handle.start());
        BigInteger stop = BigInteger.valueOf(handle.stop());
        BigInteger step = BigInteger.valueOf(handle.step());

        if (step.equals(BigInteger.ZERO)) {
            checkArgument(start.equals(stop), "start is not equal to stop for step = 0");
            return new FixedSplitSource(ImmutableList.of(new SequenceFunctionSplit(start.longValueExact(), stop.longValueExact())));
        }

        ImmutableList.Builder<SequenceFunctionSplit> splits = ImmutableList.builder();

        BigInteger totalSteps = stop.subtract(start).divide(step).add(BigInteger.ONE);
        BigInteger totalSplits = totalSteps.divide(BigInteger.valueOf(MAX_SPLIT_SIZE)).add(BigInteger.ONE);
        BigInteger[] stepsPerSplit = totalSteps.divideAndRemainder(totalSplits);
        BigInteger splitJump = stepsPerSplit[0].subtract(BigInteger.ONE).multiply(step);

        BigInteger splitStart = start;
        for (BigInteger i = BigInteger.ZERO; i.compareTo(totalSplits) < 0; i = i.add(BigInteger.ONE)) {
            BigInteger splitStop = splitStart.add(splitJump);
            // distribute the remaining steps between the initial splits, one step per split
            if (i.compareTo(stepsPerSplit[1]) < 0) {
                splitStop = splitStop.add(step);
            }
            splits.add(new SequenceFunctionSplit(splitStart.longValueExact(), splitStop.longValueExact()));
            splitStart = splitStop.add(step);
        }

        return new FixedSplitSource(splits.build());
    }

    public static class SequenceFunctionSplit
            implements ConnectorSplit
    {
        public static final int DEFAULT_SPLIT_SIZE = 1000000;
        public static final int MAX_SPLIT_SIZE = 1000000;

        // the first value of sub-sequence
        private final long start;

        // the last value of sub-sequence. this value is aligned so that it belongs to the sequence.
        private final long stop;

        @JsonCreator
        public SequenceFunctionSplit(@JsonProperty("start") long start, @JsonProperty("stop") long stop)
        {
            this.start = start;
            this.stop = stop;
        }

        @JsonProperty
        public long getStart()
        {
            return start;
        }

        @JsonProperty
        public long getStop()
        {
            return stop;
        }

        @Override
        public NodeSelectionStrategy getNodeSelectionStrategy()
        {
            return NO_PREFERENCE;
        }

        @Override
        public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
        {
            return Collections.emptyList();
        }

        @Override
        public Object getInfo()
        {
            return ImmutableMap.builder()
                    .put("start", start)
                    .put("stop", stop)
                    .buildOrThrow();
        }
    }

    public static TableFunctionProcessorProvider getSequenceFunctionProcessorProvider()
    {
        return new TableFunctionProcessorProvider() {
            @Override
            public TableFunctionSplitProcessor getSplitProcessor(ConnectorTableFunctionHandle handle)
            {
                return new SequenceFunctionProcessor(((SequenceFunctionHandle) handle).step());
            }
        };
    }

    public static class SequenceFunctionProcessor
            implements TableFunctionSplitProcessor
    {
        private final PageBuilder page = new PageBuilder(ImmutableList.of(BIGINT));
        private final long step;
        private long start;
        private long stop;
        private boolean finished;

        public SequenceFunctionProcessor(long step)
        {
            this.step = step;
        }

        @Override
        public TableFunctionProcessorState process(ConnectorSplit split)
        {
            if (split != null) {
                SequenceFunctionSplit sequenceSplit = (SequenceFunctionSplit) split;
                start = sequenceSplit.getStart();
                stop = sequenceSplit.getStop();
                BlockBuilder block = page.getBlockBuilder(0);
                while (start != stop && !page.isFull()) {
                    page.declarePosition();
                    BIGINT.writeLong(block, start);
                    start += step;
                }
                if (!page.isFull()) {
                    page.declarePosition();
                    BIGINT.writeLong(block, start);
                    finished = true;
                    return usedInputAndProduced(page.build());
                }
                return usedInputAndProduced(page.build());
            }

            if (finished) {
                return FINISHED;
            }

            page.reset();
            BlockBuilder block = page.getBlockBuilder(0);
            while (start != stop && !page.isFull()) {
                page.declarePosition();
                BIGINT.writeLong(block, start);
                start += step;
            }
            if (!page.isFull()) {
                page.declarePosition();
                BIGINT.writeLong(block, start);
                finished = true;
                return produced(page.build());
            }
            return produced(page.build());
        }
    }
}
