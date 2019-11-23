/**
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
package com.victoriametrics.presto

import com.facebook.presto.spi.Plugin
import com.facebook.presto.spi.connector.ConnectorContext
import com.facebook.presto.spi.connector.ConnectorFactory
import com.victoriametrics.presto.model.VmConfig
import com.victoriametrics.presto.model.VmHandleResolver
import com.victoriametrics.presto.model.VmSchema

class VictoriaMetricsPlugin : Plugin {
    override fun getConnectorFactories() = listOf(VmConnectorFactory())

    class VmConnectorFactory : ConnectorFactory {
        override fun getName() = VmSchema.connectorName

        override fun getHandleResolver() = VmHandleResolver

        override fun create(
            catalogName: String,
            config: Map<String, String>,
            context: ConnectorContext
        ): VmConnector {
            val vmConfig = VmConfig(config)
            return VmConnector(vmConfig)
        }
    }
}


