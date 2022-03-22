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

public class NoopTracer
        implements Tracer
{
    @Override
    public void addPoint(String annotation)
    {
    }

    @Override
    public void startBlock(String blockName, String annotation)
    {
    }

    @Override
    public void addPointToBlock(String blockName, String annotation)
    {
    }

    @Override
    public void endBlock(String blockName, String annotation)
    {
    }

    @Override
    public void endTrace(String annotation)
    {
    }
}
