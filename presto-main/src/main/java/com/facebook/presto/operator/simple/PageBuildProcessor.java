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

import com.facebook.presto.operator.simple.SimpleOperator.ProcessorState;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ClassToInstanceMap;
import com.google.common.collect.ImmutableClassToInstanceMap;

import javax.annotation.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Processor adapter that enables output via a managed PageBuilder.
 * <p>
 * <pre>
 * State Machine:
 *                  _______________
 *                  |             |
 *                  v             |
 *       START::NEEDS_INPUT -> HAS_OUTPUT
 *                  |
 *                  |
 *                  -----> DRAIN_PROCESSOR ------
 *                  |             |             |
 *                  |             v             |
 *                  -------> FINAL_FLUSH        |
 *                  |             |             |
 *                  |             v             |
 *                  ---------> FINISHED <--------
 * </pre>
 */
public final class PageBuildProcessor<T>
        implements ProcessorInput<T>, ProcessorPageOutput
{
    private final StateContext<T> stateContext;

    public PageBuildProcessor(List<Type> types, ProcessorInput<T> input, ProcessorPageBuilderOutput pageBuilderOutput)
    {
        this.stateContext = new StateContext<>(new Context<>(types, input, pageBuilderOutput));
    }

    @Override
    public ProcessorState addInput(@Nullable T input)
    {
        return stateContext.getState().addInput(stateContext, input);
    }

    /**
     * Forcibly transition to either NEEDS_INPUT or HAS_OUTPUT
     */
    public void outOfBandTransition(ProcessorState processorState)
    {
        stateContext.getState().outOfBandTransition(stateContext, processorState);
    }

    @Override
    public Page extractOutput()
    {
        return stateContext.getState().extractOutput(stateContext);
    }

    private interface State<T>
    {
        default ProcessorState addInput(StateContext<T> stateContext, @Nullable T input)
        {
            throw new UnsupportedOperationException();
        }

        default void outOfBandTransition(StateContext<T> stateContext, ProcessorState processorState)
        {
            throw new UnsupportedOperationException();
        }

        default Page extractOutput(StateContext<T> stateContext)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class NeedsInputState<T>
            implements State<T>
    {
        @Override
        public ProcessorState addInput(StateContext<T> stateContext, @Nullable T input)
        {
            Context<T> context = stateContext.getContext();
            ProcessorState processorState = context.getInput().addInput(input);
            switch (processorState) {
                case NEEDS_INPUT:
                    if (input == null) {
                        if (!context.getPageBuilder().isEmpty()) {
                            stateContext.transitionTo(FinalFlushState.class);
                            return ProcessorState.HAS_OUTPUT;
                        }
                        else {
                            stateContext.transitionTo(FinishedState.class);
                        }
                    }
                    break;

                case HAS_OUTPUT:
                    if (input == null) {
                        stateContext.transitionTo(DrainProcessorState.class);
                    }
                    else {
                        stateContext.transitionTo(HasOutputState.class);
                    }
                    break;
            }
            return processorState;
        }

        @Override
        public void outOfBandTransition(StateContext<T> stateContext, ProcessorState processorState)
        {
            if (processorState == ProcessorState.HAS_OUTPUT) {
                stateContext.transitionTo(HasOutputState.class);
            }
        }
    }

    private static class HasOutputState<T>
            implements State<T>
    {
        @Override
        public void outOfBandTransition(StateContext<T> stateContext, ProcessorState processorState)
        {
            if (processorState == ProcessorState.NEEDS_INPUT) {
                stateContext.transitionTo(NeedsInputState.class);
            }
        }

        @Override
        public Page extractOutput(StateContext<T> stateContext)
        {
            Context<T> context = stateContext.getContext();
            if (!stateContext.getContext().getPageBuilder().isFull() && context.getOutput().appendOutputTo(context.getPageBuilder()) == ProcessorState.NEEDS_INPUT) {
                stateContext.transitionTo(NeedsInputState.class);
                return null;
            }
            return buildAndReset(context.getPageBuilder());
        }
    }

    private static class DrainProcessorState<T>
            implements State<T>
    {
        @Override
        public Page extractOutput(StateContext<T> stateContext)
        {
            Context<T> context = stateContext.getContext();
            if (!stateContext.getContext().getPageBuilder().isFull() && context.getOutput().appendOutputTo(context.getPageBuilder()) == ProcessorState.NEEDS_INPUT) {
                if (context.getPageBuilder().isEmpty()) {
                    stateContext.transitionTo(FinishedState.class);
                    return null;
                }
                else {
                    stateContext.transitionTo(FinalFlushState.class);
                }
            }
            return buildAndReset(context.getPageBuilder());
        }
    }

    private static class FinalFlushState<T>
            implements State<T>
    {
        @Override
        public Page extractOutput(StateContext<T> stateContext)
        {
            Context<T> context = stateContext.getContext();
            if (context.getPageBuilder().isEmpty()) {
                stateContext.transitionTo(FinishedState.class);
                return null;
            }
            return buildAndReset(context.getPageBuilder());
        }
    }

    private static class FinishedState<T>
            implements State<T>
    {
    }

    private static class StateContext<T>
    {
        private final Context<T> context;
        private final ClassToInstanceMap<State<T>> stateMap;
        private State<T> state;

        public StateContext(Context<T> context)
        {
            this.context = requireNonNull(context, "context is null");
            stateMap = ImmutableClassToInstanceMap.<State<T>>builder()
                    .put(NeedsInputState.class, new NeedsInputState<T>())
                    .put(HasOutputState.class, new HasOutputState<T>())
                    .put(DrainProcessorState.class, new DrainProcessorState<T>())
                    .put(FinalFlushState.class, new FinalFlushState<T>())
                    .put(FinishedState.class, new FinishedState<T>())
                    .build();
            state = stateMap.get(NeedsInputState.class);
        }

        public State<T> getState()
        {
            return state;
        }

        @SuppressWarnings("rawtypes")
        public void transitionTo(Class<? extends State> newState)
        {
            //noinspection SuspiciousMethodCalls
            state = stateMap.get(newState);
        }

        public Context<T> getContext()
        {
            return context;
        }
    }

    private static class Context<T>
    {
        private final ProcessorInput<T> input;
        private final ProcessorPageBuilderOutput output;
        private final PageBuilder pageBuilder;

        public Context(List<Type> types, ProcessorInput<T> input, ProcessorPageBuilderOutput output)
        {
            requireNonNull(types, "types is null");
            requireNonNull(input, "input is null");
            requireNonNull(output, "output is null");

            this.input = input;
            this.output = output;
            this.pageBuilder = new PageBuilder(types);
        }

        public ProcessorInput<T> getInput()
        {
            return input;
        }

        public ProcessorPageBuilderOutput getOutput()
        {
            return output;
        }

        public PageBuilder getPageBuilder()
        {
            return pageBuilder;
        }
    }

    private static Page buildAndReset(PageBuilder pageBuilder)
    {
        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }
}
