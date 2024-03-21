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
package com.facebook.presto.operator;

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.server.DynamicFilterService;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class InMemoryDynamicFilterClientSupplier
        implements DynamicFilterClientSupplier
{
    private final DynamicFilterService service;

    @Inject
    public InMemoryDynamicFilterClientSupplier(DynamicFilterService service)
    {
        this.service = requireNonNull(service, "dynamicFilterService is null");
    }

    @Override
    public DynamicFilterClient createClient(TaskId taskId, String source, int driverId, int expectedDriversCount, TypeManager manager)
    {
        return new InMemoryDynamicFilterClient(service, taskId, source, driverId, expectedDriversCount);
    }

    @Override
    public DynamicFilterClient createClient(TypeManager manager)
    {
        throw new UnsupportedOperationException("Not supported");
    }
}
