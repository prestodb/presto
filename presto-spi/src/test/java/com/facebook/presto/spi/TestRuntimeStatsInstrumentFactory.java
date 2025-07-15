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
package com.facebook.presto.spi;

import com.facebook.presto.common.RuntimeStatsInstrument;
import com.facebook.presto.common.TestRuntimeStatsInstrument;

import java.util.Map;

public class TestRuntimeStatsInstrumentFactory
        implements RuntimeStatsInstrumentFactory
{
    @Override
    public String getName()
    {
        return "test_instrument";
    }

    @Override
    public RuntimeStatsInstrument createRuntimeStatsInstrument(Map<String, String> config)
    {
        return new TestRuntimeStatsInstrument(config);
    }
}
