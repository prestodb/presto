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

import com.facebook.presto.memory.TraversingQueryContextVisitor;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.TaskContext;

import java.util.Collection;
import java.util.List;

public class MemoryRevokingSchedulerUtils
{
    private MemoryRevokingSchedulerUtils() {}

    public static long getMemoryAlreadyBeingRevoked(Collection<TaskContext> taskContexts, long targetRevokingLimit)
    {
        TraversingQueryContextVisitor<Void, Long> visitor = new TraversingQueryContextVisitor<Void, Long>()
        {
            @Override
            public Long visitOperatorContext(OperatorContext operatorContext, Void context)
            {
                if (operatorContext.isMemoryRevokingRequested()) {
                    return operatorContext.getReservedRevocableBytes();
                }
                return 0L;
            }

            @Override
            public Long mergeResults(List<Long> childrenResults)
            {
                return childrenResults.stream()
                        .mapToLong(i -> i).sum();
            }
        };
        long currentRevoking = 0;
        for (TaskContext taskContext : taskContexts) {
            currentRevoking += taskContext.accept(visitor, null);
            if (currentRevoking > targetRevokingLimit) {
                // Return early, target value exceeded and revoking will not occur
                return currentRevoking;
            }
        }
        return currentRevoking;
    }
}
