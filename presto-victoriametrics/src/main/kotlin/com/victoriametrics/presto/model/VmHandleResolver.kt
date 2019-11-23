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
package com.victoriametrics.presto.model

import com.facebook.presto.spi.ConnectorHandleResolver

object VmHandleResolver : ConnectorHandleResolver {
    override fun getSplitClass() = VmSplit::class.java
    override fun getTableHandleClass() = VmTableHandle::class.java
    override fun getTableLayoutHandleClass() = VmTableLayoutHandle::class.java
    override fun getColumnHandleClass() = VmColumnHandle::class.java
    override fun getTransactionHandleClass() = VmTransactionHandle::class.java
}
