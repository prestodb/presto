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
package io.prestosql.orc;

import io.airlift.slice.OutputStreamSliceOutput;
import io.prestosql.orc.stream.OrcDataOutput;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class OutputStreamOrcDataSink
        implements OrcDataSink
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(OutputStreamOrcDataSink.class).instanceSize();

    private final OutputStreamSliceOutput output;

    public OutputStreamOrcDataSink(OutputStream outputStream)
    {
        this.output = new OutputStreamSliceOutput(requireNonNull(outputStream, "outputStream is null"));
    }

    @Override
    public long size()
    {
        return output.longSize();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + output.getRetainedSize();
    }

    @Override
    public void write(List<OrcDataOutput> outputData)
    {
        outputData.forEach(data -> data.writeData(output));
    }

    @Override
    public void close()
            throws IOException
    {
        output.close();
    }
}
