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
package io.prestosql.plugin.accumulo;

import com.google.common.base.Splitter;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;

import java.io.File;
import java.lang.reflect.Field;
import java.util.List;

import static java.lang.management.ManagementFactory.getRuntimeMXBean;

public final class MiniAccumuloConfigUtil
{
    private MiniAccumuloConfigUtil() {}

    /**
     * MiniAccumuloClusterImpl will build the class path itself if not set,
     * but the code fails on Java 9 due to assumptions about URLClassLoader.
     */
    public static void setConfigClassPath(MiniAccumuloConfig config)
    {
        List<String> items = Splitter.on(File.pathSeparatorChar)
                .splitToList(getRuntimeMXBean().getClassPath());
        getConfigImpl(config).setClasspathItems(items.toArray(new String[0]));
    }

    private static MiniAccumuloConfigImpl getConfigImpl(MiniAccumuloConfig config)
    {
        try {
            Field field = MiniAccumuloConfig.class.getDeclaredField("impl");
            field.setAccessible(true);
            return (MiniAccumuloConfigImpl) field.get(config);
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }
}
