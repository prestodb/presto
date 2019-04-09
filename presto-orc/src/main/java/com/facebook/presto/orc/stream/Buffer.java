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

public class Buffer
{
    private byte[] buffer;
    private int position;
    private int length;

    public byte[] getBuffer()
    {
        return buffer;
    }

    public int getPosition()
    {
        return position;
    }

    public int getLength()
    {
        return length;
    }

    public void setBuffer(byte[] buffer)
    {
        this.buffer = buffer;
    }

    public void setPosition(int position)
    {
        this.position = position;
    }

    public void setLength(int length)
    {
        this.length = length;
    }
}
