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
package com.facebook.presto.execution;

import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.ValuesOperator;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.split.ConnectorDataStreamProvider;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TestingDataStreamProvider
        implements ConnectorDataStreamProvider
{
    @Override
    public Operator createNewDataStream(OperatorContext operatorContext, ConnectorSplit split, List<ConnectorColumnHandle> columns)
    {
        checkNotNull(operatorContext, "operatorContext is null");
        checkNotNull(columns, "columns is null");
        checkArgument(split instanceof TestingSplit, "split is not a TestingSplit");

        // TODO: check for !columns.isEmpty() -- currently, it breaks TestSqlTaskManager
        // and fixing it requires allowing TableScan nodes with no assignments

        return new ValuesOperator(operatorContext, ImmutableList.of(new Page(1)));
    }
}
