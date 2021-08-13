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

import com.facebook.presto.metadata.InsertTableHandle;
import com.facebook.presto.metadata.OutputTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import static java.util.Objects.requireNonNull;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ExecutionWriterTarget.CreateHandle.class, name = "CreateHandle"),
        @JsonSubTypes.Type(value = ExecutionWriterTarget.InsertHandle.class, name = "InsertHandle"),
        @JsonSubTypes.Type(value = ExecutionWriterTarget.DeleteHandle.class, name = "DeleteHandle"),
        @JsonSubTypes.Type(value = ExecutionWriterTarget.RefreshMaterializedViewHandle.class, name = "RefreshMaterializedViewHandle")})
@SuppressWarnings({"EmptyClass", "ClassMayBeInterface"})
public abstract class ExecutionWriterTarget
{
    public static class CreateHandle
            extends ExecutionWriterTarget
    {
        private final OutputTableHandle handle;
        private final SchemaTableName schemaTableName;

        @JsonCreator
        public CreateHandle(
                @JsonProperty("handle") OutputTableHandle handle,
                @JsonProperty("schemaTableName") SchemaTableName schemaTableName)
        {
            this.handle = requireNonNull(handle, "handle is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        }

        @JsonProperty
        public OutputTableHandle getHandle()
        {
            return handle;
        }

        @JsonProperty
        public SchemaTableName getSchemaTableName()
        {
            return schemaTableName;
        }

        @Override
        public String toString()
        {
            return handle.toString();
        }
    }

    public static class InsertHandle
            extends ExecutionWriterTarget
    {
        private final InsertTableHandle handle;
        private final SchemaTableName schemaTableName;

        @JsonCreator
        public InsertHandle(
                @JsonProperty("handle") InsertTableHandle handle,
                @JsonProperty("schemaTableName") SchemaTableName schemaTableName)
        {
            this.handle = requireNonNull(handle, "handle is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        }

        @JsonProperty
        public InsertTableHandle getHandle()
        {
            return handle;
        }

        @JsonProperty
        public SchemaTableName getSchemaTableName()
        {
            return schemaTableName;
        }

        @Override
        public String toString()
        {
            return handle.toString();
        }
    }

    public static class DeleteHandle
            extends ExecutionWriterTarget
    {
        private final TableHandle handle;
        private final SchemaTableName schemaTableName;

        @JsonCreator
        public DeleteHandle(
                @JsonProperty("handle") TableHandle handle,
                @JsonProperty("schemaTableName") SchemaTableName schemaTableName)
        {
            this.handle = requireNonNull(handle, "handle is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        }

        @JsonProperty
        public TableHandle getHandle()
        {
            return handle;
        }

        @JsonProperty
        public SchemaTableName getSchemaTableName()
        {
            return schemaTableName;
        }

        @Override
        public String toString()
        {
            return handle.toString();
        }
    }

    public static class RefreshMaterializedViewHandle
            extends ExecutionWriterTarget
    {
        private final InsertTableHandle handle;
        private final SchemaTableName schemaTableName;

        @JsonCreator
        public RefreshMaterializedViewHandle(
                @JsonProperty("handle") InsertTableHandle handle,
                @JsonProperty("schemaTableName") SchemaTableName schemaTableName)
        {
            this.handle = requireNonNull(handle, "handle is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        }

        @JsonProperty
        public InsertTableHandle getHandle()
        {
            return handle;
        }

        @JsonProperty
        public SchemaTableName getSchemaTableName()
        {
            return schemaTableName;
        }

        @Override
        public String toString()
        {
            return handle.toString();
        }
    }
}
