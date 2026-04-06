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
package com.facebook.presto.plugin.oracle;

import com.facebook.presto.plugin.jdbc.JdbcMetadataFactory;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

/**
 * Override module for Oracle metadata factory binding.
 * This module overrides the default JdbcMetadataFactory binding from JdbcModule
 * to use OracleMetadataFactory instead.
 */
public class OracleMetadataOverrideModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        // Override the JdbcMetadataFactory binding from JdbcModule
        binder.bind(JdbcMetadataFactory.class).to(OracleMetadataFactory.class).in(Scopes.SINGLETON);
    }
}
