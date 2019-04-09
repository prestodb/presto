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
package com.facebook.presto.orc.stream;

import com.facebook.presto.orc.OrcDataSourceId;
import com.google.common.collect.PeekingIterator;

import java.io.IOException;
import java.io.InputStream;

public abstract class OrcInputStream
        extends InputStream implements PeekingIterator<Buffer>
{
    @Override
    public void close()
    {
        // close is never called, so do not add code here
    }

    @Override
    public boolean markSupported()
    {
        return false;
    }

    public abstract void skipFully(long length)
            throws IOException;

    public abstract void readFully(byte[] buffer, int offset, int length)
            throws IOException;

    public abstract OrcDataSourceId getOrcDataSourceId();

    public abstract long getCheckpoint();

    public abstract boolean seekToCheckpoint(long checkpoint)
            throws IOException;

    protected abstract void advance()
            throws IOException;
}
