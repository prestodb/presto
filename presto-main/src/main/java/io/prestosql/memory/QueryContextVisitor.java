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
package io.prestosql.memory;

import io.prestosql.operator.DriverContext;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.PipelineContext;
import io.prestosql.operator.TaskContext;

public interface QueryContextVisitor<C, R>
{
    R visitQueryContext(QueryContext queryContext, C visitContext);

    R visitTaskContext(TaskContext taskContext, C visitContext);

    R visitPipelineContext(PipelineContext pipelineContext, C visitContext);

    R visitDriverContext(DriverContext driverContext, C visitContext);

    R visitOperatorContext(OperatorContext operatorContext, C visitContext);
}
