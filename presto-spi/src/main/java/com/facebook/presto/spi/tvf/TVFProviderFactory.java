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
package com.facebook.presto.spi.tvf;

import com.facebook.presto.spi.function.TableFunctionHandleResolver;
import com.facebook.presto.spi.function.TableFunctionSplitResolver;

import java.util.Map;

public interface TVFProviderFactory
{
    TVFProvider createTVFProvider(Map<String, String> config, TVFProviderContext context);

    TableFunctionHandleResolver getTableFunctionHandleResolver();

    TableFunctionSplitResolver getTableFunctionSplitResolver();

    String getName();
}
