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
package com.facebook.presto.hive;

import com.facebook.presto.execution.TaskId;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.ConnectorSession;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;

public final class HiveTestUtils
{
    private HiveTestUtils()
    {
    }

    public static void close(Operator dataStream)
            throws IOException
    {
        if (dataStream instanceof Closeable) {
            ((Closeable) dataStream).close();
        }
    }

    public static OperatorContext creteOperatorContext(ConnectorSession session, ExecutorService executor)
    {
        return new TaskContext(new TaskId("query", "stage", "task"), executor, session).addPipelineContext(true, true).addDriverContext().addOperatorContext(0, "test");
    }
}
