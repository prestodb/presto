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

import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.memory.context.LocalMemoryContext;

import java.util.HashMap;
import java.util.Map;

public class MergeJoinSourceManager
{
    private final Map<Lifespan, MergeJoinSource> mergeJoinSourceMap;
    private final boolean bufferEnabled;

    public MergeJoinSourceManager(boolean bufferEnabled)
    {
        this.mergeJoinSourceMap = new HashMap<>();
        this.bufferEnabled = bufferEnabled;
    }

    public MergeJoinSource getMergeJoinSource(Lifespan lifespan, LocalMemoryContext localMemoryContext)
    {
        synchronized (this) {
            if (!mergeJoinSourceMap.containsKey(lifespan)) {
                mergeJoinSourceMap.put(lifespan, new MergeJoinSource(bufferEnabled));
            }
            if (localMemoryContext != null) {
                mergeJoinSourceMap.get(lifespan).setLocalMemoryContext(localMemoryContext);
            }
            return mergeJoinSourceMap.get(lifespan);
        }
    }
}
