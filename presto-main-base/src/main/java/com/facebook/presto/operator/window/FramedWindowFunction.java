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

import com.facebook.presto.spi.function.WindowFunction;

import static java.util.Objects.requireNonNull;

public final class FramedWindowFunction
{
    private final WindowFunction function;
    private final FrameInfo frame;

    public FramedWindowFunction(WindowFunction windowFunction, FrameInfo frameInfo)
    {
        this.function = requireNonNull(windowFunction, "windowFunction is null");
        this.frame = requireNonNull(frameInfo, "frameInfo is null");
    }

    public WindowFunction getFunction()
    {
        return function;
    }

    public FrameInfo getFrame()
    {
        return frame;
    }
}
