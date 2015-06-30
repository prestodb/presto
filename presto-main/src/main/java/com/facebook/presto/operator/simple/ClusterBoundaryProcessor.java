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

import com.facebook.presto.operator.PagePositionEqualitor;
import com.facebook.presto.operator.Pages;
import com.facebook.presto.operator.simple.SimpleOperator.ProcessorState;
import com.facebook.presto.spi.Page;
import io.airlift.units.DataSize;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Provides callbacks between cluster boundaries after a minimum of minClusterCallbackSize of input Pages.
 *
 * <pre>
 * State Machine:
 *
 *       START::PASS_THROUGH -> OUTPUT_DRAIN_BEFORE_BOUNDARY_SEEK
 *              ^   |     ^                    ^     |   ^
 *              |   |     |                    |     |   |
 *              |   |     -------  ____  -------     |   |
 *              |   |           |  |  |  |           |   |
 *              |   |           |  |  v  |           |   |
 *              |   ---------> BOUNDARY_SEEK <--------   |
 *              |                  |  ^                  |
 *              |                  |  |                  |
 *              |                  v  |                  |
 *              -- OUTPUT_DRAIN_BEFORE_PROCESS_FRAGMENT --
 *
 * </pre>
 */
public class ClusterBoundaryProcessor
        implements ProcessorInput<Page>, ProcessorPageOutput
{
    private final StateContext stateContext;

    public ClusterBoundaryProcessor(DataSize minClusterCallbackSize, ProcessorInput<Page> input, ProcessorPageOutput pageOutput, ClusterBoundaryCallback clusterBoundaryCallback, PagePositionEqualitor equalitor)
    {
        this.stateContext = new StateContext(new Context(minClusterCallbackSize, input, pageOutput, clusterBoundaryCallback, equalitor));
    }

    @Override
    public ProcessorState addInput(@Nullable Page input)
    {
        return stateContext.getState().addInput(stateContext, input);
    }

    @Override
    public Page extractOutput()
    {
        return stateContext.getState().extractOutput(stateContext);
    }

    private enum State
    {
        PASS_THROUGH {
            @Override
            public void inboundTransition(Context context)
            {
                checkState(context.getReferencePage() == null);
                checkState(context.getPageFragment() == null);
            }

            @Override
            public void outboundTransition(Context context)
            {
                checkState(context.getReferencePage() != null);
            }

            @Override
            public ProcessorState addInput(StateContext stateContext, @Nullable Page input)
            {
                if (input == null) {
                    return stateContext.getContext().getInput().addInput(null);
                }
                else {
                    return processInputAndTransition(stateContext, input);
                }
            }

            @Override
            public Page extractOutput(StateContext stateContext)
            {
                return stateContext.getContext().getOutput().extractOutput();
            }
        },

        OUTPUT_DRAIN_BEFORE_BOUNDARY_SEEK {
            @Override
            public void inboundTransition(Context context)
            {
                checkState(context.getReferencePage() != null);
                checkState(context.getPageFragment() == null);
            }

            @Override
            public Page extractOutput(StateContext stateContext)
            {
                Page page = stateContext.getContext().getOutput().extractOutput();
                if (page == null) {
                    stateContext.transitionTo(State.BOUNDARY_SEEK);
                }
                return page;
            }
        },

        BOUNDARY_SEEK {
            @Override
            public void inboundTransition(Context context)
            {
                checkState(context.getReferencePage() != null);
                checkState(context.getPageFragment() == null);
            }

            @Override
            public ProcessorState addInput(StateContext stateContext, @Nullable Page input)
            {
                Context context = stateContext.getContext();

                if (input == null) {
                    context.setReferencePage(null); // At the last page, so don't need reference anymore
                    context.resetCurrentBytes();
                    stateContext.transitionTo(State.PASS_THROUGH);
                    return context.getInput().addInput(null);
                }
                else {
                    int clusterEnd = Pages.findClusterEnd(context.getReferencePage(), context.getReferencePage().getPositionCount() - 1, input, 0, context.getEqualitor());
                    if (clusterEnd <= 0) {
                        // Input is entirely new cluster of values
                        context.setReferencePage(null);
                        context.resetCurrentBytes();
                        return notifyBoundaryAndProcessFragment(stateContext, input);
                    }
                    else if (clusterEnd == input.getPositionCount()) {
                        // Everything in this page matches the last row in the reference page, need more input to find the boundary
                        ProcessorState state = context.getInput().addInput(input);
                        if (state == ProcessorState.HAS_OUTPUT) {
                            stateContext.transitionTo(State.OUTPUT_DRAIN_BEFORE_BOUNDARY_SEEK);
                        }
                        return state;
                    }
                    else {
                        // Found a cluster boundary inside the page
                        context.setReferencePage(null);
                        context.resetCurrentBytes();
                        ProcessorState state = context.getInput().addInput(input.getRegion(0, clusterEnd));
                        Page fragment = input.getRegion(clusterEnd, input.getPositionCount() - clusterEnd);

                        switch (state) {
                            case NEEDS_INPUT:
                                return notifyBoundaryAndProcessFragment(stateContext, fragment);

                            case HAS_OUTPUT:
                                context.setPageFragment(fragment);
                                stateContext.transitionTo(State.OUTPUT_DRAIN_BEFORE_PROCESS_FRAGMENT);
                                return ProcessorState.HAS_OUTPUT;

                            default:
                                throw new AssertionError();
                        }
                    }
                }
            }

            private ProcessorState notifyBoundaryAndProcessFragment(StateContext stateContext, Page nextFragment)
            {
                Context context = stateContext.getContext();
                ProcessorState state = context.getClusterBoundaryCallback().clusterBoundary();
                switch (state) {
                    case NEEDS_INPUT:
                        return processInputAndTransition(stateContext, nextFragment);

                    case HAS_OUTPUT:
                        context.setPageFragment(nextFragment);
                        stateContext.transitionTo(State.OUTPUT_DRAIN_BEFORE_PROCESS_FRAGMENT);
                        break;
                }
                return state;
            }
        },

        OUTPUT_DRAIN_BEFORE_PROCESS_FRAGMENT {
            @Override
            public void inboundTransition(Context context)
            {
                checkState(context.getReferencePage() == null);
                checkState(context.getPageFragment() != null);
            }

            @Override
            public void outboundTransition(Context context)
            {
                checkState(context.getPageFragment() == null);
            }

            @Override
            public Page extractOutput(StateContext stateContext)
            {
                Context context = stateContext.getContext();

                Page page = context.getOutput().extractOutput();
                if (page == null) {
                    Page fragment = context.getPageFragment();
                    context.setPageFragment(null);
                    ProcessorState state = processInputAndTransition(stateContext, fragment);
                    if (state == ProcessorState.HAS_OUTPUT) {
                        Page output = context.getOutput().extractOutput();
                        if (output == null && context.shouldFlush()) {
                            stateContext.transitionTo(State.BOUNDARY_SEEK);
                        }
                        return output;
                    }
                }
                return page;
            }
        };

        public void inboundTransition(Context context)
        {
        }

        public void outboundTransition(Context context)
        {
        }

        public ProcessorState addInput(StateContext stateContext, @Nullable Page input)
        {
            throw new UnsupportedOperationException();
        }

        public Page extractOutput(StateContext stateContext)
        {
            throw new UnsupportedOperationException();
        }

        private static ProcessorState processInputAndTransition(StateContext stateContext, Page input)
        {
            Context context = stateContext.getContext();

            context.addBytes(input.getSizeInBytes());
            ProcessorState state = context.getInput().addInput(input);
            switch (state) {
                case NEEDS_INPUT:
                    if (context.shouldFlush()) {
                        context.setReferencePage(input);
                        stateContext.transitionTo(State.BOUNDARY_SEEK);
                    }
                    else {
                        stateContext.transitionTo(State.PASS_THROUGH);
                    }
                    break;
                case HAS_OUTPUT:
                    if (context.shouldFlush()) {
                        context.setReferencePage(input);
                        stateContext.transitionTo(State.OUTPUT_DRAIN_BEFORE_BOUNDARY_SEEK);
                    }
                    else {
                        stateContext.transitionTo(State.PASS_THROUGH);
                    }
                    break;
            }
            return state;
        }
    }

    private static class StateContext
    {
        private final Context context;
        private State state = State.PASS_THROUGH;

        public StateContext(Context context)
        {
            this.context = requireNonNull(context, "context is null");
        }

        public State getState()
        {
            return state;
        }

        public void transitionTo(State newState)
        {
            if (newState != state) {
                state.outboundTransition(context);
                newState.inboundTransition(context);
                state = newState;
            }
        }

        public Context getContext()
        {
            return context;
        }
    }

    private static class Context
    {
        private final DataSize minClusterCallbackSize;
        private final ProcessorInput<Page> input;
        private final ProcessorPageOutput output;
        private final ClusterBoundaryCallback clusterBoundaryCallback;
        private final PagePositionEqualitor equalitor;

        private long currentBytes;
        private Page referencePage;
        private Page pageFragment;

        public Context(DataSize minClusterCallbackSize, ProcessorInput<Page> input, ProcessorPageOutput output, ClusterBoundaryCallback clusterBoundaryCallback, PagePositionEqualitor equalitor)
        {
            requireNonNull(minClusterCallbackSize, "minClusterCallbackSize is null");
            requireNonNull(input, "input is null");
            requireNonNull(output, "output is null");
            requireNonNull(clusterBoundaryCallback, "clusterBoundaryNotifier is null");
            requireNonNull(equalitor, "equalitor is null");

            this.minClusterCallbackSize = minClusterCallbackSize;
            this.input = input;
            this.output = output;
            this.clusterBoundaryCallback = clusterBoundaryCallback;
            this.equalitor = equalitor;
        }

        public boolean shouldFlush()
        {
            return currentBytes >= minClusterCallbackSize.toBytes();
        }

        public void resetCurrentBytes()
        {
            currentBytes = 0;
        }

        public void addBytes(long bytes)
        {
            currentBytes += bytes;
        }

        public Page getReferencePage()
        {
            return referencePage;
        }

        public void setReferencePage(Page lastPage)
        {
            this.referencePage = lastPage;
        }

        public Page getPageFragment()
        {
            return pageFragment;
        }

        public void setPageFragment(Page pageFragment)
        {
            this.pageFragment = pageFragment;
        }

        public ProcessorInput<Page> getInput()
        {
            return input;
        }

        public ProcessorPageOutput getOutput()
        {
            return output;
        }

        public ClusterBoundaryCallback getClusterBoundaryCallback()
        {
            return clusterBoundaryCallback;
        }

        public PagePositionEqualitor getEqualitor()
        {
            return equalitor;
        }
    }
}
