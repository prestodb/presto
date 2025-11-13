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
package com.facebook.presto.execution.scheduler;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftUnion;
import com.facebook.drift.annotations.ThriftUnionId;

import static java.util.Objects.requireNonNull;

@ThriftUnion
public class ExecutionWriterTargetUnion
{
    private short id;
    private ExecutionWriterTarget.CreateHandle createHandle;
    private ExecutionWriterTarget.InsertHandle insertHandle;
    private ExecutionWriterTarget.DeleteHandle deleteHandle;
    private ExecutionWriterTarget.RefreshMaterializedViewHandle refreshMaterializedViewHandle;
    private ExecutionWriterTarget.UpdateHandle updateHandle;
    private ExecutionWriterTarget.MergeHandle mergeHandle;

    @ThriftConstructor
    public ExecutionWriterTargetUnion()
    {
        this.id = 0;
    }

    @ThriftConstructor
    public ExecutionWriterTargetUnion(ExecutionWriterTarget.CreateHandle createHandle)
    {
        this.id = 1;
        this.createHandle = createHandle;
    }

    @ThriftField(1)
    public ExecutionWriterTarget.CreateHandle getCreateHandle()
    {
        return createHandle;
    }

    @ThriftConstructor
    public ExecutionWriterTargetUnion(ExecutionWriterTarget.InsertHandle insertHandle)
    {
        this.id = 2;
        this.insertHandle = insertHandle;
    }

    @ThriftField(2)
    public ExecutionWriterTarget.InsertHandle getInsertHandle()
    {
        return insertHandle;
    }

    @ThriftConstructor
    public ExecutionWriterTargetUnion(ExecutionWriterTarget.DeleteHandle deleteHandle)
    {
        this.id = 3;
        this.deleteHandle = deleteHandle;
    }

    @ThriftField(3)
    public ExecutionWriterTarget.DeleteHandle getDeleteHandle()
    {
        return deleteHandle;
    }

    @ThriftConstructor
    public ExecutionWriterTargetUnion(ExecutionWriterTarget.RefreshMaterializedViewHandle refreshMaterializedViewHandle)
    {
        this.id = 4;
        this.refreshMaterializedViewHandle = refreshMaterializedViewHandle;
    }

    @ThriftField(4)
    public ExecutionWriterTarget.RefreshMaterializedViewHandle getRefreshMaterializedViewHandle()
    {
        return refreshMaterializedViewHandle;
    }

    @ThriftConstructor
    public ExecutionWriterTargetUnion(ExecutionWriterTarget.UpdateHandle updateHandle)
    {
        this.id = 5;
        this.updateHandle = updateHandle;
    }

    @ThriftField(5)
    public ExecutionWriterTarget.UpdateHandle getUpdateHandle()
    {
        return updateHandle;
    }

    @ThriftConstructor
    public ExecutionWriterTargetUnion(ExecutionWriterTarget.MergeHandle mergeHandle)
    {
        this.id = 6;
        this.mergeHandle = mergeHandle;
    }

    @ThriftField(6)
    public ExecutionWriterTarget.MergeHandle getMergeHandle()
    {
        return mergeHandle;
    }

    @ThriftUnionId
    public short getId()
    {
        return id;
    }

    public static ExecutionWriterTarget toExecutionWriterTarget(ExecutionWriterTargetUnion executionWriterTargetUnion)
    {
        requireNonNull(executionWriterTargetUnion, "executionWriterTargetUnion is null");
        if (executionWriterTargetUnion.getCreateHandle() != null) {
            return executionWriterTargetUnion.getCreateHandle();
        }
        else if (executionWriterTargetUnion.getInsertHandle() != null) {
            return executionWriterTargetUnion.getInsertHandle();
        }
        else if (executionWriterTargetUnion.getDeleteHandle() != null) {
            return executionWriterTargetUnion.getDeleteHandle();
        }
        else if (executionWriterTargetUnion.getRefreshMaterializedViewHandle() != null) {
            return executionWriterTargetUnion.getRefreshMaterializedViewHandle();
        }
        else if (executionWriterTargetUnion.getUpdateHandle() != null) {
            return executionWriterTargetUnion.getUpdateHandle();
        }
        else if (executionWriterTargetUnion.getMergeHandle() != null) {
            return executionWriterTargetUnion.getMergeHandle();
        }
        else {
            throw new IllegalArgumentException("Unrecognized execution writer target: " + executionWriterTargetUnion);
        }
    }

    public static ExecutionWriterTargetUnion fromExecutionWriterTarget(ExecutionWriterTarget executionWriterTarget)
    {
        requireNonNull(executionWriterTarget, "executionWriterTarget is null");

        if (executionWriterTarget instanceof ExecutionWriterTarget.CreateHandle) {
            return new ExecutionWriterTargetUnion((ExecutionWriterTarget.CreateHandle) executionWriterTarget);
        }
        else if (executionWriterTarget instanceof ExecutionWriterTarget.InsertHandle) {
            return new ExecutionWriterTargetUnion((ExecutionWriterTarget.InsertHandle) executionWriterTarget);
        }
        else if (executionWriterTarget instanceof ExecutionWriterTarget.DeleteHandle) {
            return new ExecutionWriterTargetUnion((ExecutionWriterTarget.DeleteHandle) executionWriterTarget);
        }
        else if (executionWriterTarget instanceof ExecutionWriterTarget.RefreshMaterializedViewHandle) {
            return new ExecutionWriterTargetUnion((ExecutionWriterTarget.RefreshMaterializedViewHandle) executionWriterTarget);
        }
        else if (executionWriterTarget instanceof ExecutionWriterTarget.UpdateHandle) {
            return new ExecutionWriterTargetUnion((ExecutionWriterTarget.UpdateHandle) executionWriterTarget);
        }
        else if (executionWriterTarget instanceof ExecutionWriterTarget.MergeHandle) {
            return new ExecutionWriterTargetUnion((ExecutionWriterTarget.MergeHandle) executionWriterTarget);
        }
        else {
            throw new IllegalArgumentException("Unsupported execution writer target: " + executionWriterTarget);
        }
    }
}
