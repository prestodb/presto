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
package com.facebook.presto.benchmark;

import java.util.Map;

public class RenamingBenchmark
        extends AbstractBenchmark
{
    private final AbstractBenchmark delegate;

    public RenamingBenchmark(String prefix, AbstractBenchmark delegate)
    {
        super(prefix + delegate.getBenchmarkName(), delegate.getWarmupIterations(), delegate.getMeasuredIterations());
        this.delegate = delegate;
    }

    @Override
    protected Map<String, Long> runOnce()
    {
        return delegate.runOnce();
    }
}
