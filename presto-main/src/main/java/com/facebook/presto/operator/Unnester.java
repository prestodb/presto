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

import com.facebook.presto.spi.PageBuilder;

public abstract class Unnester
{
    private final int channelCount;

    protected Unnester(int channelCount)
    {
        this.channelCount = channelCount;
    }

    public final int getChannelCount()
    {
        return channelCount;
    }

    public final void appendNext(PageBuilder pageBuilder, int outputChannelOffset)
    {
        appendTo(pageBuilder, outputChannelOffset);
    }

    protected abstract void appendTo(PageBuilder pageBuilder, int outputChannelOffset);

    public abstract boolean hasNext();
}
