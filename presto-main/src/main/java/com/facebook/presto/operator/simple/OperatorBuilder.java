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
package com.facebook.presto.operator.simple;

import com.facebook.presto.operator.MultiPageChunkCursor;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.PagePositionEqualitor;
import com.facebook.presto.operator.simple.SimpleOperator.ProcessorState;
import com.facebook.presto.spi.Page;
import io.airlift.units.DataSize;

import java.util.function.LongSupplier;

import static java.util.Objects.requireNonNull;

public final class OperatorBuilder
{
    private final ProcessorBase processorBase;

    private ProcessorInput<Page> input;

    private ProcessorPageOutput pageOutput;
    private ProcessorPageBuilderOutput pageBuilderOutput;

    private LongSupplier memorySizeSupplier;

    private PagePositionEqualitor clusterBoundaryEqualitor;
    private DataSize clusterBoundaryMinCallbackSize;
    private ClusterBoundaryCallback clusterBoundaryCallback;

    public OperatorBuilder(ProcessorBase processorBase)
    {
        this.processorBase = requireNonNull(processorBase, "processorBase is null");
    }

    public static OperatorBuilder newOperatorBuilder(ProcessorBase processorBase)
    {
        return new OperatorBuilder(processorBase);
    }

    public OperatorBuilder bindInput(ProcessorInput<Page> input)
    {
        this.input = requireNonNull(input, "input is null");
        return this;
    }

    public OperatorBuilder bindClusteredInput(PagePositionEqualitor equalitor, ProcessorInput<MultiPageChunkCursor> input)
    {
        this.input = new ClusteredProcessorInput(equalitor, input);
        return this;
    }

    public OperatorBuilder bindOutput(ProcessorPageOutput output)
    {
        this.pageOutput = requireNonNull(output, "output is null");
        this.pageBuilderOutput = null;
        return this;
    }

    public OperatorBuilder bindOutput(ProcessorPageBuilderOutput output)
    {
        this.pageOutput = null;
        this.pageBuilderOutput = requireNonNull(output, "output is null");
        return this;
    }

    public OperatorBuilder withMemoryTracking(LongSupplier memorySizeSupplier)
    {
        this.memorySizeSupplier = requireNonNull(memorySizeSupplier, "memorySizeSupplier is null");
        return this;
    }

    public OperatorBuilder withClusterBoundaryCallback(PagePositionEqualitor equalitor, DataSize minCallbackSize, ClusterBoundaryCallback callback)
    {
        clusterBoundaryEqualitor = requireNonNull(equalitor, "equalitor is null");
        clusterBoundaryMinCallbackSize = requireNonNull(minCallbackSize, "minCallbackSize is null");
        clusterBoundaryCallback = requireNonNull(callback, "callback is null");
        return this;
    }

    public Operator build()
    {
        ProcessorInput<Page> processorInput;
        ProcessorPageOutput operatorOutput;

        if (pageOutput == null) {
            PageBuildProcessor<Page> pageBuildProcessor = new PageBuildProcessor<>(processorBase.getTypes(), input, pageBuilderOutput);
            processorInput = pageBuildProcessor;
            operatorOutput = pageBuildProcessor;

            if (clusterBoundaryCallback != null) {
                // clusterBoundaryCallback can trigger state transitions, so notify PageBuildProcessor when it happens
                // TODO: figure out how to do this more generally
                clusterBoundaryCallback = notifyInline(clusterBoundaryCallback, pageBuildProcessor);
            }
        }
        else {
            processorInput = input;
            operatorOutput = pageOutput;
        }

        if (clusterBoundaryEqualitor != null) {
            ClusterBoundaryProcessor clusterBoundaryProcessor = new ClusterBoundaryProcessor(clusterBoundaryMinCallbackSize, processorInput, operatorOutput, clusterBoundaryCallback, clusterBoundaryEqualitor);
            processorInput = clusterBoundaryProcessor;
            operatorOutput = clusterBoundaryProcessor;
        }

        if (memorySizeSupplier != null) {
            processorInput = MemoryAccounting.wrap(processorBase.getOperatorContext(), processorInput, memorySizeSupplier);
            operatorOutput = MemoryAccounting.wrap(processorBase.getOperatorContext(), operatorOutput, memorySizeSupplier);
        }

        return new SimpleOperator(processorBase, processorInput, operatorOutput);
    }

    private static ClusterBoundaryCallback notifyInline(ClusterBoundaryCallback callback, PageBuildProcessor<Page> pageBuildProcessor)
    {
        return () -> {
            ProcessorState state = callback.clusterBoundary();
            pageBuildProcessor.outOfBandTransition(state);
            return state;
        };
    }
}
