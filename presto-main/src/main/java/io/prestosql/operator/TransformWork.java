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
package io.prestosql.operator;

import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class TransformWork<I, O>
        implements Work<O>
{
    private final Work<I> work;
    private final Function<I, O> transformation;

    private boolean finished;
    private O result;

    public TransformWork(Work<I> work, Function<I, O> transformation)
    {
        this.work = requireNonNull(work, "work is null");
        this.transformation = requireNonNull(transformation, "transformation is null");
    }

    @Override
    public boolean process()
    {
        checkState(!finished);
        finished = work.process();
        if (!finished) {
            return false;
        }
        result = transformation.apply(work.getResult());
        return true;
    }

    @Override
    public O getResult()
    {
        checkState(finished, "process has not finished");
        return result;
    }
}
