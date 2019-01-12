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
package io.prestosql.spi.eventlistener;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class QueryIOMetadata
{
    private final List<QueryInputMetadata> inputs;
    private final Optional<QueryOutputMetadata> output;

    public QueryIOMetadata(List<QueryInputMetadata> inputs, Optional<QueryOutputMetadata> output)
    {
        this.inputs = requireNonNull(inputs, "inputs is null");
        this.output = requireNonNull(output, "output is null");
    }

    public List<QueryInputMetadata> getInputs()
    {
        return inputs;
    }

    public Optional<QueryOutputMetadata> getOutput()
    {
        return output;
    }
}
