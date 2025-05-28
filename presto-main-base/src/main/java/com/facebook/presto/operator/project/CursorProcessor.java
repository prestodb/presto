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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.spi.RecordCursor;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.concurrent.ConcurrentHashMap;

public interface CursorProcessor
{
    CursorProcessorOutput process(SqlFunctionProperties properties,
                                  DriverYieldSignal yieldSignal, RecordCursor cursor, PageBuilder pageBuilder);

    Logger log = Logger.get(CursorProcessor.class);
    MethodType METHOD_TYPE = MethodType.methodType(
            void.class,
            SqlFunctionProperties.class,
            RecordCursor.class,
            BlockBuilder.class);

    ConcurrentHashMap<String, MethodHandle> METHOD_HANDLE_CACHE = new ConcurrentHashMap<>();

    default void doProjection(int projectionIndex, SqlFunctionProperties properties,
                              RecordCursor cursor, PageBuilder pageBuilder)
    {
        final String methodName = "project_" + projectionIndex;
        final String cacheKey = this.getClass().getName() + "#" + methodName;

        MethodHandle methodHandle = METHOD_HANDLE_CACHE.computeIfAbsent(cacheKey, key -> {
            try {
                return MethodHandles.lookup()
                        .findVirtual(this.getClass(), methodName, METHOD_TYPE)
                        .bindTo(this);
            }
            catch (NoSuchMethodException e) {
                log.error("Projection method not found: {}", methodName, e);
                throw new RuntimeException("Projection method not found: " + methodName, e);
            }
            catch (IllegalAccessException e) {
                log.error("Cannot access projection method: {}", methodName, e);
                throw new RuntimeException("Cannot access projection method: " + methodName, e);
            }
        });

        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(projectionIndex);
        try {
            methodHandle.invokeExact(properties, cursor, blockBuilder);
        }
        catch (RuntimeException | Error e) {
            throw e;
        }
        catch (Throwable e) {
            log.error("Failed to invoke projection method: {}", methodName, e);
            throw new RuntimeException("Failed to invoke projection method: " + methodName, e);
        }
    }
}
