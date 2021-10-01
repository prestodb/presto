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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.google.common.base.MoreObjects.toStringHelper;

public class SimpleTracerBlock
{
    private final List<SimpleTracerPoint> points = new CopyOnWriteArrayList<>();
    private final long startTime = System.nanoTime();
    private long endTime;

    public SimpleTracerBlock(String annotation)
    {
        addPoint(new SimpleTracerPoint(annotation));
    }

    public long getStartTime()
    {
        return startTime;
    }

    public void end(String annotation)
    {
        endTime = System.nanoTime();
        if (startTime > endTime) {
            throw new RuntimeException("Block start time " + startTime + " is greater than end time " + endTime);
        }
        addPoint(new SimpleTracerPoint(annotation));
    }

    public long getEndTime()
    {
        return endTime;
    }

    public void addPoint(SimpleTracerPoint point)
    {
        points.add(point);
    }

    public String toString()
    {
        return toStringHelper(this)
                .add("start time", startTime)
                .add("end time", endTime)
                .add("points", points)
                .toString();
    }
}
