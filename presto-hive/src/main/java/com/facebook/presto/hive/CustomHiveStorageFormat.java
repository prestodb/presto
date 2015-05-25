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
package com.facebook.presto.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;

import static com.facebook.presto.hive.util.Types.checkType;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Class.forName;

public class CustomHiveStorageFormat implements HiveStorageFormat
{
    private final String serde;
    private final String inputFormat;
    private final String outputFormat;

    @JsonCreator
    public CustomHiveStorageFormat(
            @JsonProperty("serDe") String serDe,
            @JsonProperty("inputFormat") String inputFormat,
            @JsonProperty("outputFormat") String outputFormat)
    {
        this.serde = checkNotNull(serDe, "serDe is null");
        this.inputFormat = checkNotNull(inputFormat, "inputFormat is null");
        this.outputFormat = checkNotNull(outputFormat, "outputFormat is null");
    }

    public CustomHiveStorageFormat(HiveStorageHandler storageHandler)
    {
        this(storageHandler.getSerDeClass().getName(),
                storageHandler.getInputFormatClass().getName(),
                storageHandler.getOutputFormatClass().getName());
    }

    @JsonProperty
    @Override
    public String getSerDe()
    {
        return serde;
    }

    @JsonProperty
    @Override
    public String getInputFormat()
    {
        return inputFormat;
    }

    @JsonProperty
    @Override
    public String getOutputFormat()
    {
        return outputFormat;
    }

    public static HiveStorageFormat valueOf(String storageClassName) throws ClassNotFoundException, IllegalAccessException, InstantiationException
    {
        Class<?> clazz = forName(storageClassName);
        Object storageHandler = clazz.newInstance();
        checkType(storageHandler, HiveStorageHandler.class, "storageHandler");
        return new CustomHiveStorageFormat((HiveStorageHandler) storageHandler);
    }
}
