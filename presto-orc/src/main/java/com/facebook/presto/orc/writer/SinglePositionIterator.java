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
package com.facebook.presto.orc.writer;

public class SinglePositionIterator
        implements PositionIterator
{
    private int position;

    @Override
    public int next()
    {
        return position;
    }

    @Override
    public int getPositionCount()
    {
        return 1;
    }

    @Override
    public void reset()
    {
    }

    @Override
    public void clear()
    {

    }

    public void set(int position)
    {
        this.position = position;
    }
}
