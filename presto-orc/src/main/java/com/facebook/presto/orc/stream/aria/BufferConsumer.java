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
package com.facebook.presto.orc.stream.aria;

import java.io.IOException;

public abstract class BufferConsumer
{
    protected byte[] buffer;
    protected int length;
    protected int position;

    public void setBuffer(byte[] buffer, int length)
    {
        this.buffer = buffer;
        this.length = length;
        this.position = 0;
    }

    public abstract void skip(int items)
            throws IOException;

    protected abstract void refresh()
            throws IOException;

    public int length()
    {
        return length;
    }

    public int remaining()
            throws IOException
    {
        if (length - position == 0) {
            refresh();
        }
        return length - position;
    }

    public int read()
            throws IOException
    {
        if (remaining() == 0) {
            return -1;
        }
        return 0xff & buffer[position++];
    }
}
