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
package com.facebook.presto.iceberg;

import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.google.inject.Binder;
import com.google.inject.Module;

import javax.inject.Inject;

public class IcebergMetastoreModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(MetastoreValidator.class).asEagerSingleton();
    }

    public static class MetastoreValidator
    {
        @Inject
        public MetastoreValidator(ExtendedHiveMetastore metastore)
        {
        }
    }
}
