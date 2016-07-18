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
package com.facebook.presto.hive.benchmark;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@AuxCounters
@State(Scope.Thread)
public class CompressionCounter
{
    private long inputSize;
    private long outputSize;

    @Setup(Level.Iteration)
    public void reset()
    {
        inputSize = 0;
        outputSize = 0;
    }

    public void addCompressed(long inputSize, long outputsize)
    {
        this.inputSize += inputSize;
        this.outputSize += outputsize;
    }

    public long getInputSize()
    {
        return inputSize;
    }

    public long getOutputSize()
    {
        return outputSize;
    }
}
