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
package io.prestosql.decoder;

import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

public class DispatchingRowDecoderFactory
{
    private final Map<String, RowDecoderFactory> factories;

    @Inject
    public DispatchingRowDecoderFactory(Map<String, RowDecoderFactory> factories)
    {
        this.factories = ImmutableMap.copyOf(factories);
    }

    public RowDecoder create(String dataFormat, Map<String, String> decoderParams, Set<DecoderColumnHandle> columns)
    {
        checkArgument(factories.containsKey(dataFormat), "unknown data format '%s'", dataFormat);
        return factories.get(dataFormat).create(decoderParams, columns);
    }
}
