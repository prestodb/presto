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

import com.facebook.presto.plugin.turbonium.TurboniumConfig;
import com.facebook.presto.plugin.turbonium.config.db.TurboniumConfigDao;
import com.facebook.presto.plugin.turbonium.config.db.TurboniumConfigSpec;
import com.facebook.presto.spi.NodeManager;
import com.google.common.base.Throwables;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.lang.Math.toIntExact;

public class TurboniumConfigManager
{
    private static final Logger log = Logger.get(TurboniumConfigManager.class);
    private final AtomicReference<TurboniumConfig> config = new AtomicReference<>();
    private final TurboniumConfigDao dao;
    private final boolean canWrite;

    @Inject
    public TurboniumConfigManager(TurboniumConfigDao dao, TurboniumConfig initialConfig, NodeManager nodeManager)
    {
        this.dao = dao;
        config.set(initialConfig);
        canWrite = nodeManager.getCurrentNode().isCoordinator();
        dao.createConfigTable();
        load();
    }

    public TurboniumConfig getConfig()
    {
        return config.get();
    }

    public TurboniumConfigSpec getStaticConfig()
    {
        TurboniumConfig config = this.config.get();
        return new TurboniumConfigSpec(config.getMaxDataPerNode().toBytes(),
                config.getMaxTableSizePerNode().toBytes(),
                config.getSplitsPerNode(),
                config.getDisableEncoding());
    }

    public void setFromConfig(TurboniumConfigSpec config)
    {
        TurboniumConfig newConfig = new TurboniumConfig()
                .setMaxDataPerNode(new DataSize(config.getMaxDataPerNode(), BYTE))
                .setMaxTableSizePerNode(new DataSize(config.getMaxTableSizePerNode(), BYTE))
                .setSplitsPerNode(toIntExact(config.getSplitsPerNode()))
                .setDisableEncoding(config.getDisableEncoding());

        this.config.set(newConfig);
    }

    private synchronized void load()
    {
        try {
            List<TurboniumConfig> turboniumConfig = dao.getMemoryConfig();
            if (turboniumConfig.isEmpty()) {
                if (canWrite) {
                    TurboniumConfig currentConfig = config.get();
                    dao.insertMemoryConfig(
                            currentConfig.getMaxDataPerNode().toBytes(),
                            currentConfig.getMaxTableSizePerNode().toBytes(),
                            currentConfig.getSplitsPerNode(),
                            currentConfig.getDisableEncoding());
                }
            }
            else {
                config.set(getOnlyElement(turboniumConfig));
            }
        }
        catch (Exception e) {
            log.error("Failed to load config: %s", e);
            e.printStackTrace();
        }
    }

    public synchronized void setMaxDataPerNode(long maxDataSizePerNode)
    {
        config.set(copyMemoryConfig().setMaxDataPerNode(new DataSize(maxDataSizePerNode, BYTE)));
        try {
            if (canWrite) {
                dao.updateMaxDataPerNode(maxDataSizePerNode);
            }
        }
        catch (Exception e) {
            log.error("Failed to update max data segmentCount per node: %s", e);
            Throwables.propagate(e);
        }
    }

    public synchronized void setMaxTableSizePerNode(long maxTableSizePerNode)
    {
        config.set(copyMemoryConfig().setMaxTableSizePerNode(new DataSize(maxTableSizePerNode, BYTE)));
        try {
            if (canWrite) {
                dao.updateMaxTableSizePerNode(maxTableSizePerNode);
            }
        }
        catch (Exception e) {
            log.error("Failed to update max table segmentCount per node: %s", e);
            Throwables.propagate(e);
        }
    }

    public synchronized void setSplitsPerNode(int splitsPerNode)
    {
        config.set(copyMemoryConfig().setSplitsPerNode(splitsPerNode));
        try {
            if (canWrite) {
                dao.updateSplitsPerNode(splitsPerNode);
            }
        }
        catch (Exception e) {
            log.error("Failed to update splits per node: %s", e);
            Throwables.propagate(e);
        }
    }

    public synchronized void setDisableEncoding(boolean disableEncoding)
    {
        config.set(copyMemoryConfig().setDisableEncoding(disableEncoding));
        try {
            if (canWrite) {
                dao.updateDisableEncoding(disableEncoding);
            }
        }
        catch (Exception e) {
            log.error("Failed to update disable_encoding: %s", e);
            Throwables.propagate(e);
        }
    }

    private TurboniumConfig copyMemoryConfig()
    {
        TurboniumConfig newConfig = new TurboniumConfig();
        TurboniumConfig currentConfig = config.get();
        return newConfig.setMaxDataPerNode(currentConfig.getMaxDataPerNode())
                .setMaxTableSizePerNode(currentConfig.getMaxTableSizePerNode())
                .setSplitsPerNode(currentConfig.getSplitsPerNode())
                .setDisableEncoding(currentConfig.getDisableEncoding());
    }
}
