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
package com.facebook.presto.hive.rcfile;

import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.rcfile.RcFilePageSource.RcFileColumnsBatch;
import com.facebook.presto.spi.block.LazyArrayBlock;
import com.facebook.presto.spi.block.LazyBlockLoader;
import com.facebook.presto.spi.block.LazyFixedWidthBlock;
import com.facebook.presto.spi.block.LazySliceArrayBlock;
import com.facebook.presto.spi.type.Type;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public interface RcFileBlockLoader
{
    LazyBlockLoader<LazyFixedWidthBlock> fixedWidthBlockLoader(RcFileColumnsBatch batch, int fieldId, HiveType hiveType);

    LazyBlockLoader<LazySliceArrayBlock> variableWidthBlockLoader(RcFileColumnsBatch batch, int fieldId, HiveType hiveType, ObjectInspector fieldInspector, Type type);

    LazyBlockLoader<LazyArrayBlock> structuralBlockLoader(RcFileColumnsBatch batch, int fieldId, HiveType hiveType, ObjectInspector fieldInspector, Type type);
}
