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

import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;

import javax.annotation.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Operator implementation framework that provides a simpler processor model.
 * <p>
 * <pre>
 * SimpleOperator State Machine:
 *                  _______________
 *                  |             |
 *                  v             |
 *       START::NEEDS_INPUT -> HAS_OUTPUT
 *                  |             |
 *                  |             v
 *                  |       FINISH_OUTPUT
 *                  |             |
 *                  |             v
 *                  -------> DRAIN_PROCESSOR
 *                                |
 *                                v
 *                             FINISHED
 * </pre>
 * <p>
 * NEEDS_INPUT:
 */
public final class SimpleOperator
        implements Operator
{
    /**
     * Processor implementations will only have two possible states:
     * 1) NEEDS_INPUT - input will be supplied until the processor returns HAS_OUTPUT. Null input indicates end of input.
     * 2) HAS_OUTPUT - output will be extracted until a null Page is received, thereby transitioning back to NEEDS_INPUT.
     * <p>
     * <pre>
     * Processor State Machine:
     *                  _______________
     *                  |             |
     *                  v             |
     *       START::NEEDS_INPUT -> HAS_OUTPUT
     *
     * </pre>
     */
    private static class Processor
    {
        private final ProcessorBase base;
        private final ProcessorInput<Page> processorInput;
        private final ProcessorPageOutput processorOutput;

        public Processor(ProcessorBase base, ProcessorInput<Page> processorInput, ProcessorPageOutput processorOutput)
        {
            this.base = requireNonNull(base, "base is null");
            this.processorInput = requireNonNull(processorInput, "processorInput is null");
            this.processorOutput = requireNonNull(processorOutput, "processorOutput is null");
        }

        public String getName()
        {
            return base.getName();
        }

        public List<Type> getTypes()
        {
            return base.getTypes();
        }

        public OperatorContext getOperatorContext()
        {
            return base.getOperatorContext();
        }

        public ProcessorState addInput(@Nullable Page input)
        {
            return processorInput.addInput(input);
        }

        public Page extractOutput()
        {
            return processorOutput.extractOutput();
        }

        public void close()
                throws Exception
        {
            base.close();
        }
    }

    public enum ProcessorState
    {
        NEEDS_INPUT,
        HAS_OUTPUT
    }

    private final Context context;

    public SimpleOperator(ProcessorBase base, ProcessorInput<Page> processorInput, ProcessorPageOutput processorOutput)
    {
        requireNonNull(base, "base is null");
        requireNonNull(processorInput, "processorInput is null");
        requireNonNull(processorOutput, "processorOutput is null");

        this.context = new Context(new Processor(base, processorInput, processorOutput));
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return context.getProcessor().getOperatorContext();
    }

    @Override
    public List<Type> getTypes()
    {
        return context.getProcessor().getTypes();
    }

    @Override
    public void finish()
    {
        context.getState().finish(context);
    }

    @Override
    public boolean isFinished()
    {
        return context.getState().isFinished();
    }

    @Override
    public boolean needsInput()
    {
        return context.getState().needsInput();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        context.getState().addInput(context, page);
    }

    @Override
    public Page getOutput()
    {
        return context.getState().getOutput(context);
    }

    @Override
    public void close()
            throws Exception
    {
        context.close();
    }

    @Override
    public String toString()
    {
        return String.format("%s:%s:[%s]", SimpleOperator.class.getSimpleName(), context.getState(), context.getProcessor().getName());
    }

    private static class Context
    {
        private State state = State.NEEDS_INPUT;
        private final Processor processor;

        public Context(Processor processor)
        {
            this.processor = requireNonNull(processor, "processor is null");
        }

        public State getState()
        {
            return state;
        }

        public void transitionTo(State newState)
        {
            state = newState;
        }

        public Processor getProcessor()
        {
            return processor;
        }

        public void close()
                throws Exception
        {
            processor.close();
        }
    }

    private enum State
    {
        NEEDS_INPUT {
            @Override
            public void addInput(Context context, Page page)
            {
                if (page.getPositionCount() == 0) {
                    return;
                }

                if (context.getProcessor().addInput(page) == ProcessorState.HAS_OUTPUT) {
                    context.transitionTo(HAS_OUTPUT);
                }
            }

            @Override
            public Page getOutput(Context context)
            {
                return null;
            }

            @Override
            public void finish(Context context)
            {
                switch (context.getProcessor().addInput(null)) {
                    case HAS_OUTPUT:
                        context.transitionTo(DRAIN_PROCESSOR);
                        break;

                    case NEEDS_INPUT:
                        context.transitionTo(FINISHED);
                        break;
                }
            }

            @Override
            public boolean needsInput()
            {
                return true;
            }
        },

        HAS_OUTPUT {
            @Override
            public void addInput(Context context, Page page)
            {
                throw new UnsupportedOperationException("Don't need input");
            }

            @Override
            public Page getOutput(Context context)
            {
                Page page = context.getProcessor().extractOutput();
                if (page == null) {
                    context.transitionTo(NEEDS_INPUT);
                }
                return page;
            }

            @Override
            public void finish(Context context)
            {
                context.transitionTo(FINISH_OUTPUT);
            }
        },

        FINISH_OUTPUT {
            @Override
            public void addInput(Context context, Page page)
            {
                throw new UnsupportedOperationException("Already finishing");
            }

            @Override
            public Page getOutput(Context context)
            {
                Page page = context.getProcessor().extractOutput();
                if (page == null) {
                    switch (context.getProcessor().addInput(null)) {
                        case HAS_OUTPUT:
                            context.transitionTo(DRAIN_PROCESSOR);
                            break;

                        case NEEDS_INPUT:
                            context.transitionTo(FINISHED);
                            break;
                    }
                }
                return page;
            }

            @Override
            public void finish(Context context)
            {
                // Finish can be called multiple times
            }

            @Override
            public String toString()
            {
                return "FLUSHING";
            }
        },

        DRAIN_PROCESSOR {
            @Override
            public void addInput(Context context, Page page)
            {
                throw new UnsupportedOperationException("Already finishing");
            }

            @Override
            public Page getOutput(Context context)
            {
                Page page = context.getProcessor().extractOutput();
                if (page == null) {
                    context.transitionTo(FINISHED);
                }
                return page;
            }

            @Override
            public void finish(Context context)
            {
                // Finish can be called multiple times
            }

            @Override
            public String toString()
            {
                return "FLUSHING";
            }
        },

        FINISHED {
            @Override
            public void addInput(Context context, Page page)
            {
                throw new UnsupportedOperationException("Already finished");
            }

            @Override
            public Page getOutput(Context context)
            {
                return null;
            }

            @Override
            public void finish(Context context)
            {
                // Finish can be called multiple times
            }

            @Override
            public boolean isFinished()
            {
                return true;
            }
        };

        public abstract void addInput(Context context, Page page);

        public abstract Page getOutput(Context context);

        public abstract void finish(Context context);

        public boolean needsInput()
        {
            return false;
        }

        public boolean isFinished()
        {
            return false;
        }
    }
}
