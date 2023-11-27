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
package com.facebook.presto.common;

import com.facebook.presto.common.QueryTypeAndExecutionExtraMessage.ExecutionExtraMessage;

import java.util.List;

public class QueryTypeAndExecutionExtraMessage<T extends ExecutionExtraMessage>
{
    final String updateType;
    final T executionExtraMessage;

    public QueryTypeAndExecutionExtraMessage(
            String updateType,
            T executionExtraMessage)
    {
        this.updateType = updateType;
        this.executionExtraMessage = executionExtraMessage;
    }

    public String getUpdateType()
    {
        return updateType;
    }

    public T getExecutionExtraMessage()
    {
        return executionExtraMessage;
    }

    public static interface ExecutionExtraMessage
    {}

    public static class ExtraMessageForExecuteBatch
            implements ExecutionExtraMessage
    {
        final int rowsForEachBatchLine;

        public ExtraMessageForExecuteBatch(int rowsForEachBatchLine)
        {
            this.rowsForEachBatchLine = rowsForEachBatchLine;
        }

        public int getRowsForEachBatchLine()
        {
            return rowsForEachBatchLine;
        }
    }

    public static class ExtraMessageForPrepare
            implements ExecutionExtraMessage
    {
        final boolean insertValuesWithParameter;
        final List<String> typesForValidate;

        public ExtraMessageForPrepare(
                boolean insertValuesWithParameter,
                List<String> typesForValidate)
        {
            this.insertValuesWithParameter = insertValuesWithParameter;
            this.typesForValidate = typesForValidate;
        }

        public boolean isInsertValuesWithParameter()
        {
            return this.insertValuesWithParameter;
        }

        public List<String> getTypesForValidate()
        {
            return typesForValidate;
        }
    }
}
