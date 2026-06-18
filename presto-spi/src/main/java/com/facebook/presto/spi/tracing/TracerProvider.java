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
package com.facebook.presto.spi.tracing;

import java.util.Map;
import java.util.function.Function;

public interface TracerProvider
{
    String getName();

    /**
     * Return tracer type provided by TracerProvider
     */
    String getTracerType();

    /**
     * The method returns a function to take a set of HTTP headers and generate the tracer handle
     */
    Function<Map<String, String>, TracerHandle> getHandleGenerator();

    /**
     *
     * @return A @Tracer that should be kept throughout the whole duration of tracing.
     */
    Tracer getNewTracer(TracerHandle handle);
}
