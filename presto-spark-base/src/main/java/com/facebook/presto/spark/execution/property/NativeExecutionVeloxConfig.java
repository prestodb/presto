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
package com.facebook.presto.spark.execution.property;

import com.facebook.airlift.configuration.Config;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * This config class corresponds to velox.properties for native execution process. Properties inside will be used in Configs::BaseVeloxQueryConfig in Configs.h/cpp
 */
public class NativeExecutionVeloxConfig
{
    private static final String CODEGEN_ENABLED = "codegen.enabled";

    private boolean codegenEnabled;

    public Map<String, String> getAllProperties()
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        return builder.put(CODEGEN_ENABLED, String.valueOf(getCodegenEnabled()))
                .build();
    }

    public boolean getCodegenEnabled()
    {
        return codegenEnabled;
    }

    @Config(CODEGEN_ENABLED)
    public NativeExecutionVeloxConfig setCodegenEnabled(boolean codegenEnabled)
    {
        this.codegenEnabled = codegenEnabled;
        return this;
    }
}
