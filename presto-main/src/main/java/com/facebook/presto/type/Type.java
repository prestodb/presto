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
package com.facebook.presto.type;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockBuilderStatus;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.type.TypeJacksonModule.TypeDeserializer;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.airlift.slice.Slice;

@JsonDeserialize(using = TypeDeserializer.class)
public interface Type
{
    @JsonValue
    String getName();

    Class<?> getJavaType();

    Object getObjectValue(Slice slice, int offset);

    BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus);

    ColumnType toColumnType();
}
