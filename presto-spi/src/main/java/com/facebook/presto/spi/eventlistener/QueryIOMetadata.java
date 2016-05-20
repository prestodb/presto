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

package com.facebook.presto.spi.eventlistener;

import java.util.List;

public class QueryIOMetadata
{
    private final List<QueryInputMetadata> inputs;
    private final List<String> outputs;
    private final String inputsPayload;
    private final String outputsPayload;

    public QueryIOMetadata(List<QueryInputMetadata> inputs, List<String> outputs, String inputsPayload, String outputsPayload)
    {
        this.inputs = inputs;
        this.outputs = outputs;
        this.inputsPayload = inputsPayload;
        this.outputsPayload = outputsPayload;
    }

    public List<QueryInputMetadata> getInputs()
    {
        return inputs;
    }

    public List<String> getOutputs()
    {
        return outputs;
    }

    public String getInputsPayload()
    {
        return inputsPayload;
    }

    public String getOutputsPayload()
    {
        return outputsPayload;
    }
}
