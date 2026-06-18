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

import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkQueryExecutionFactory;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkService;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkTaskExecutorFactory;
import com.facebook.presto.spark.execution.task.PrestoSparkNativeTaskExecutorFactory;
import com.facebook.presto.spark.execution.task.PrestoSparkTaskExecutorFactory;
import jakarta.inject.Inject;

import static java.util.Objects.requireNonNull;

public class PrestoSparkService
        implements IPrestoSparkService
{
    private final PrestoSparkQueryExecutionFactory queryExecutionFactory;
    private final PrestoSparkTaskExecutorFactory taskExecutorFactory;
    private final PrestoSparkNativeTaskExecutorFactory prestoSparkNativeTaskExecutorFactory;
    private final LifeCycleManager lifeCycleManager;

    @Inject
    public PrestoSparkService(
            PrestoSparkQueryExecutionFactory queryExecutionFactory,
            PrestoSparkTaskExecutorFactory taskExecutorFactory,
            PrestoSparkNativeTaskExecutorFactory prestoSparkNativeTaskExecutorFactory,
            LifeCycleManager lifeCycleManager)
    {
        this.queryExecutionFactory = requireNonNull(queryExecutionFactory, "queryExecutionFactory is null");
        this.taskExecutorFactory = requireNonNull(taskExecutorFactory, "taskExecutorFactory is null");
        this.prestoSparkNativeTaskExecutorFactory = requireNonNull(prestoSparkNativeTaskExecutorFactory, "prestoSparkNativeTaskExecutorFactory is null");
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
    }

    @Override
    public IPrestoSparkQueryExecutionFactory getQueryExecutionFactory()
    {
        return queryExecutionFactory;
    }

    @Override
    public IPrestoSparkTaskExecutorFactory getTaskExecutorFactory()
    {
        return taskExecutorFactory;
    }

    @Override
    public IPrestoSparkTaskExecutorFactory getNativeTaskExecutorFactory()
    {
        return prestoSparkNativeTaskExecutorFactory;
    }

    @Override
    public void close()
    {
        lifeCycleManager.stop();
        prestoSparkNativeTaskExecutorFactory.close();
        taskExecutorFactory.close();
    }
}
