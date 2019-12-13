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
package com.facebook.presto.spark;

import com.facebook.presto.spark.classloader_interface.IPrestoSparkExecutionFactory;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkService;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkTaskCompiler;
import com.facebook.presto.spark.execution.PrestoSparkTaskCompiler;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class PrestoSparkService
        implements IPrestoSparkService
{
    private final PrestoSparkExecutionFactory executionFactory;
    private final PrestoSparkTaskCompiler taskCompiler;

    @Inject
    public PrestoSparkService(PrestoSparkExecutionFactory executionFactory, PrestoSparkTaskCompiler taskCompiler)
    {
        this.executionFactory = requireNonNull(executionFactory, "executionFactory is null");
        this.taskCompiler = requireNonNull(taskCompiler, "taskCompiler is null");
    }

    @Override
    public IPrestoSparkExecutionFactory createExecutionFactory()
    {
        return executionFactory;
    }

    @Override
    public IPrestoSparkTaskCompiler createTaskCompiler()
    {
        return taskCompiler;
    }
}
