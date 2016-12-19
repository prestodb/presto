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
package com.facebook.presto.memory;

import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.PipelineContext;
import com.facebook.presto.operator.TaskContext;

import java.util.List;

public abstract class TraversingQueryContextVisitor<C, R>
        implements QueryContextVisitor<C, R>
{
    public R mergeResults(List<R> childrenResults)
    {
        return null;
    }

    @Override
    public R visitQueryContext(QueryContext queryContext, C visitContext)
    {
        return mergeResults(queryContext.acceptChildren(this, visitContext));
    }

    @Override
    public R visitTaskContext(TaskContext taskContext, C visitContext)
    {
        return mergeResults(taskContext.acceptChildren(this, visitContext));
    }

    @Override
    public R visitPipelineContext(PipelineContext pipelineContext, C visitContext)
    {
        return mergeResults(pipelineContext.acceptChildren(this, visitContext));
    }

    @Override
    public R visitDriverContext(DriverContext driverContext, C visitContext)
    {
        return mergeResults(driverContext.acceptChildren(this, visitContext));
    }

    @Override
    public R visitOperatorContext(OperatorContext operatorContext, C visitContext)
    {
        return null;
    }
}
