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
package com.facebook.presto.operator.project;

import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.spi.RecordCursor;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

public interface CursorProcessors
{
    MethodType METHOD_TYPE = MethodType.methodType(
            void.class,
            SqlFunctionProperties.class,
            RecordCursor.class,
            BlockBuilder.class);
    CursorProcessorOutput process(SqlFunctionProperties properties, DriverYieldSignal yieldSignal, RecordCursor cursor, PageBuilder pageBuilder);

    default void invokeMethod(int projectionIndex, SqlFunctionProperties properties, RecordCursor cursor, PageBuilder pageBuilder)
    {
        final String methodName = "project_" + projectionIndex;
        System.out.println("Method to invoke: " + methodName);
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(projectionIndex);
        System.out.println("blockBuilder to invoke: " + blockBuilder);
        try {
            lookup.findVirtual(this.getClass(),
                            methodName,
                            METHOD_TYPE)
                    .bindTo(this)
                    .invokeExact(properties, cursor, blockBuilder);
        }
        catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
