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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class SimpleTracerPoint
{
    private final String annotation;
    private final long time = System.nanoTime();

    public SimpleTracerPoint(String annotation)
    {
        this.annotation = requireNonNull(annotation, "annotation is null");
    }

    public String getAnnotation()
    {
        return annotation;
    }

    public long getTime()
    {
        return time;
    }

    public String toString()
    {
        return toStringHelper(this)
                .add("time", time)
                .add("annotation", annotation)
                .toString();
    }
}
