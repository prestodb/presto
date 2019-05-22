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
package com.facebook.presto.spi;

import static java.util.Objects.requireNonNull;

public class PageSourceOptions
{
    private final boolean reusePages;
    private final int[] internalChannels;
    private final int[] outputChannels;
    private final int targetBytes;

    public PageSourceOptions(int[] internalChannels,
                             int[] outputChannels,
                             boolean reusePages,
                             int targetBytes)
    {
        this.internalChannels = requireNonNull(internalChannels, "internalChannels is null");
        this.outputChannels = requireNonNull(outputChannels, "outputChannels is null");
        this.reusePages = reusePages;
        this.targetBytes = targetBytes;
    }

    public int[] getInternalChannels()
    {
        return internalChannels;
    }

    public int[] getOutputChannels()
    {
        return outputChannels;
    }

    public boolean getReusePages()
    {
        return reusePages;
    }

    public int getTargetBytes()
    {
        return targetBytes;
    }
}
