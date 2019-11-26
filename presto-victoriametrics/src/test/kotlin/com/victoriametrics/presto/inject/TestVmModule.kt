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
package com.victoriametrics.presto.inject

import com.facebook.presto.testing.TestingConnectorContext
import com.victoriametrics.presto.QueryBuilder
import com.victoriametrics.presto.model.VmConfig
import dagger.Module
import dagger.Provides
import io.mockk.mockk
import okhttp3.OkHttpClient
import javax.inject.Singleton

@Module
class TestVmModule(
        val httpClient: OkHttpClient = mockk(),
        val config: VmConfig = VmConfig(mapOf()),
        val context: TestingConnectorContext = TestingConnectorContext()
) {

    @Provides
    @Singleton
    fun provideHttpClient(): OkHttpClient {
        return httpClient
    }

    @Provides
    @Singleton
    fun provideConfig() = config

    @Provides
    @Singleton
    fun provideContext() = context

    @Provides
    fun provideQueryBuilder(config: VmConfig): QueryBuilder {
        return QueryBuilder(config.httpUrls)
    }
}
