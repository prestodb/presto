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
package com.facebook.presto.plugin.turbonium.config;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.procedure.Procedure;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.lang.invoke.MethodHandles;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class UpdateMaxTableSizePerNodeProcedure
{
    private final TurboniumConfigManager configManager;

    @Inject
    public UpdateMaxTableSizePerNodeProcedure(TurboniumConfigManager configManager)
    {
        this.configManager = requireNonNull(configManager, "configManager is null");
    }

    public void updateMaxTableSizePerNode(long maxTableSizePerNode)
    {
        configManager.setMaxTableSizePerNode(maxTableSizePerNode);
    }

    public Procedure getProcedure()
    {
        try {
            return new Procedure(
                    "system",
                    "set_max_table_size_per_node",
                    ImmutableList.of(new Procedure.Argument("max_table_size_per_node", "BIGINT")),
                    MethodHandles.lookup().unreflect(
                            getClass().getMethod("updateMaxTableSizePerNode", long.class)).bindTo(this));
        }
        catch (IllegalAccessException | NoSuchMethodException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }
}
