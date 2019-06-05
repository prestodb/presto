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
package com.facebook.presto.spi.function;

import com.facebook.presto.spi.CatalogSchemaName;
import com.facebook.presto.spi.api.Experimental;

/**
 * FunctionHandle is a unique handle to identify the function implementation from namespaces.
 * However, currently it is still under changes, so please don't assume it is backward compatible.
 */
@Experimental
public interface FunctionHandle
{
    CatalogSchemaName getCatalogSchemaName();
}
