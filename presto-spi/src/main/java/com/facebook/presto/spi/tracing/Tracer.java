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

public interface Tracer
{
    /**
     * Add a single trace point with current time to the main block
     * @param annotation message associated with the trace point
     */
    void addPoint(String annotation);

    /**
     * Add a new block starting at current time to the trace
     * @param blockName the name for the added tracing block
     * @param annotation message associated with the start point of the block
     */
    void startBlock(String blockName, String annotation);

    /**
     * Add a trace point with current time to the specified block
     * @param blockName the name for the tracing block to add point to
     * @param annotation message associated with the added point to the block
     */
    void addPointToBlock(String blockName, String annotation);

    /**
     * End the specified block
     * @param blockName the name for the tracing block to end
     * @param annotation message associated with the end point of the block
     */
    void endBlock(String blockName, String annotation);

    /**
     * End the entire trace
     * @param annotation message associated with the end point of the trace
     */
    void endTrace(String annotation);
}
