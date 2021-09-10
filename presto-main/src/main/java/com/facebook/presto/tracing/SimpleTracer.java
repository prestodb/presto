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
package com.facebook.presto.tracing;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.tracing.Tracer;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.facebook.presto.spi.StandardErrorCode.DISTRIBUTED_TRACING_ERROR;
import static com.google.common.base.MoreObjects.toStringHelper;

public class SimpleTracer
        implements Tracer
{
    public final Map<String, SimpleTracerBlock> blockMap = new ConcurrentHashMap<>();
    public final Map<String, SimpleTracerBlock> recorderBlockMap = new LinkedHashMap<>();
    public final List<SimpleTracerPoint> pointList = new CopyOnWriteArrayList<>();

    public SimpleTracer()
    {
        addPoint("Start tracing");
    }

    @Override
    public void addPoint(String annotation)
    {
        pointList.add(new SimpleTracerPoint(annotation));
    }

    @Override
    public void startBlock(String blockName, String annotation)
    {
        if (blockMap.containsKey(blockName)) {
            throw new PrestoException(DISTRIBUTED_TRACING_ERROR, "Duplicated block inserted: " + blockName);
        }
        SimpleTracerBlock block = new SimpleTracerBlock(annotation);
        blockMap.put(blockName, block);
        synchronized (recorderBlockMap) {
            recorderBlockMap.put(blockName, block);
        }
    }

    @Override
    public void addPointToBlock(String blockName, String annotation)
    {
        if (!blockMap.containsKey(blockName)) {
            throw new PrestoException(DISTRIBUTED_TRACING_ERROR, "Adding point to non-existing block: " + blockName);
        }
        blockMap.get(blockName).addPoint(new SimpleTracerPoint(annotation));
    }

    @Override
    public void endBlock(String blockName, String annotation)
    {
        if (!blockMap.containsKey(blockName)) {
            throw new PrestoException(DISTRIBUTED_TRACING_ERROR, "Trying to end a non-existing block: " + blockName);
        }
        blockMap.remove(blockName);
        synchronized (recorderBlockMap) {
            recorderBlockMap.get(blockName).end(annotation);
        }
    }

    @Override
    public void endTrace(String annotation)
    {
        pointList.add(new SimpleTracerPoint(annotation));
    }

    public String toString()
    {
        return toStringHelper(this)
                .add("points", pointList)
                .add("blocks", recorderBlockMap)
                .toString();
    }
}
