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
package com.facebook.presto.operator.window;

import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.function.WindowFunction;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public abstract class AbstractWindowFunctionSupplier
        implements WindowFunctionSupplier
{
    private final Signature signature;
    private final String description;

    protected AbstractWindowFunctionSupplier(Signature signature, String description)
    {
        this.signature = requireNonNull(signature, "signature is null");
        this.description = description;
    }

    @Override
    public final Signature getSignature()
    {
        return signature;
    }

    @Override
    public final String getDescription()
    {
        return description;
    }

    @Override
    public final WindowFunction createWindowFunction(List<Integer> argumentChannels)
    {
        requireNonNull(argumentChannels, "inputs is null");
        checkArgument(argumentChannels.size() == signature.getArgumentTypes().size(),
                "Expected %s arguments for function %s, but got %s",
                signature.getArgumentTypes().size(),
                signature.getName(),
                argumentChannels.size());

        return newWindowFunction(argumentChannels);
    }

    /**
     * Create window function instance using the supplied arguments.  The
     * inputs have already validated.
     */
    protected abstract WindowFunction newWindowFunction(List<Integer> inputs);
}
