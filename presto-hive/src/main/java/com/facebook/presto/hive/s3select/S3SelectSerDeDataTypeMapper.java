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
package com.facebook.presto.hive.s3select;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;

import java.util.Map;
import java.util.Optional;

public class S3SelectSerDeDataTypeMapper
{
    // Contains mapping of SerDe class name to corresponding data type.
    // Multiple SerDe classes can be mapped to the same data type.
    private static final Map<String, S3SelectDataType> SERDE_TO_DATA_TYPE_MAPPING = ImmutableMap.of(
            LazySimpleSerDe.class.getName(), S3SelectDataType.CSV);

    private S3SelectSerDeDataTypeMapper() {}

    public static Optional<S3SelectDataType> getDataType(String serdeName)
    {
        return Optional.ofNullable(SERDE_TO_DATA_TYPE_MAPPING.get(serdeName));
    }

    public static boolean doesSerDeExist(String serdeName)
    {
        return SERDE_TO_DATA_TYPE_MAPPING.containsKey(serdeName);
    }
}
