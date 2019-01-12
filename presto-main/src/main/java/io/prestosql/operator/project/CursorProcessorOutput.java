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
package io.prestosql.operator.project;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

public class CursorProcessorOutput
{
    private final int processedRows;
    private final boolean finished;

    public CursorProcessorOutput(int processedRows, boolean finished)
    {
        checkArgument(processedRows >= 0, "processedRows should be no smaller than 0");
        this.processedRows = processedRows;
        this.finished = finished;
    }

    public int getProcessedRows()
    {
        return processedRows;
    }

    public boolean isNoMoreRows()
    {
        return finished;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("processedRows", processedRows)
                .add("finished", finished)
                .toString();
    }
}
