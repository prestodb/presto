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
package com.facebook.presto.metadata;

import com.facebook.presto.index.IndexHandleJacksonModule;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.facebook.airlift.json.JsonBinder.jsonBinder;

public class HandleJsonModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        jsonBinder(binder).addModuleBinding().to(TableHandleJacksonModule.class);
        jsonBinder(binder).addModuleBinding().to(TableLayoutHandleJacksonModule.class);
        jsonBinder(binder).addModuleBinding().to(ColumnHandleJacksonModule.class);
        jsonBinder(binder).addModuleBinding().to(SplitJacksonModule.class);
        jsonBinder(binder).addModuleBinding().to(OutputTableHandleJacksonModule.class);
        jsonBinder(binder).addModuleBinding().to(InsertTableHandleJacksonModule.class);
        jsonBinder(binder).addModuleBinding().to(DeleteTableHandleJacksonModule.class);
        jsonBinder(binder).addModuleBinding().to(MergeTableHandleJacksonModule.class);
        jsonBinder(binder).addModuleBinding().to(DistributedProcedureHandleJacksonModule.class);
        jsonBinder(binder).addModuleBinding().to(IndexHandleJacksonModule.class);
        jsonBinder(binder).addModuleBinding().to(TransactionHandleJacksonModule.class);
        jsonBinder(binder).addModuleBinding().to(PartitioningHandleJacksonModule.class);
        jsonBinder(binder).addModuleBinding().to(FunctionHandleJacksonModule.class);
        jsonBinder(binder).addModuleBinding().to(TableFunctionJacksonHandleModule.class);

        binder.bind(HandleResolver.class).in(Scopes.SINGLETON);
    }
}
