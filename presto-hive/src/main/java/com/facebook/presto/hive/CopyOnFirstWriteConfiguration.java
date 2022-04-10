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
package com.facebook.presto.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public class CopyOnFirstWriteConfiguration
        extends Configuration
{
    private final Object lock = new Object();

    private Configuration config;
    private boolean isMutable;

    // This a wrapper class around Configuration object, that creates a copy of Configuration object on write
    public CopyOnFirstWriteConfiguration(Configuration config)
    {
        this.config = requireNonNull(config, "config is null");
        this.isMutable = false;
    }

    public Configuration getConfig()
    {
        return config;
    }

    @Override
    public void set(String name, String value)
    {
        checkAndSet(() -> config.set(name, value));
    }

    @Override
    public void set(String name, String value, String source)
    {
        checkAndSet(() -> config.set(name, value, source));
    }

    @Override
    public void setIfUnset(String name, String value)
    {
        checkAndSet(() -> config.setIfUnset(name, value));
    }

    @Override
    public void setInt(String name, int value)
    {
        checkAndSet(() -> config.setInt(name, value));
    }

    @Override
    public void setLong(String name, long value)
    {
        checkAndSet(() -> config.setLong(name, value));
    }

    @Override
    public void setFloat(String name, float value)
    {
        checkAndSet(() -> config.setFloat(name, value));
    }

    @Override
    public void setDouble(String name, double value)
    {
        checkAndSet(() -> config.setDouble(name, value));
    }

    @Override
    public void setBoolean(String name, boolean value)
    {
        checkAndSet(() -> config.setBoolean(name, value));
    }

    @Override
    public void setBooleanIfUnset(String name, boolean value)
    {
        checkAndSet(() -> config.setBooleanIfUnset(name, value));
    }

    @Override
    public <T extends Enum<T>> void setEnum(String name, T value)
    {
        checkAndSet(() -> config.setEnum(name, value));
    }

    @Override
    public void setTimeDuration(String name, long value, TimeUnit unit)
    {
        checkAndSet(() -> config.setTimeDuration(name, value, unit));
    }

    @Override
    public void setPattern(String name, Pattern pattern)
    {
        checkAndSet(() -> config.setPattern(name, pattern));
    }

    @Override
    public void setStrings(String name, String... values)
    {
        checkAndSet(() -> config.setStrings(name, values));
    }

    @Override
    public void setSocketAddr(String name, InetSocketAddress addr)
    {
        checkAndSet(() -> config.setSocketAddr(name, addr));
    }

    @Override
    public void setClass(String name, Class<?> theClass, Class<?> xface)
    {
        checkAndSet(() -> config.setClass(name, theClass, xface));
    }

    @Override
    public void setClassLoader(ClassLoader classLoader)
    {
        checkAndSet(() -> config.setClassLoader(classLoader));
    }

    @Override
    public void setQuietMode(boolean quietMode)
    {
        checkAndSet(() -> config.setQuietMode(quietMode));
    }

    @Override
    public void setDeprecatedProperties()
    {
        checkAndSet(() -> config.setDeprecatedProperties());
    }

    @Override
    public void unset(String name)
    {
        checkAndSet(() -> config.unset(name));
    }

    @Override
    public String get(String name)
    {
        return config.get(name);
    }

    @Override
    public String get(String name, String defaultValue)
    {
        return config.get(name, defaultValue);
    }

    @Override
    public String getTrimmed(String name)
    {
        return config.getTrimmed(name);
    }

    @Override
    public String getTrimmed(String name, String defaultValue)
    {
        return config.getTrimmed(name, defaultValue);
    }

    @Override
    public String getRaw(String name)
    {
        return config.getRaw(name);
    }

    @Override
    public int getInt(String name, int defaultValue)
    {
        return config.getInt(name, defaultValue);
    }

    @Override
    public int[] getInts(String name)
    {
        return config.getInts(name);
    }

    @Override
    public long getLong(String name, long defaultValue)
    {
        return config.getLong(name, defaultValue);
    }

    @Override
    public long getLongBytes(String name, long defaultValue)
    {
        return config.getLongBytes(name, defaultValue);
    }

    @Override
    public float getFloat(String name, float defaultValue)
    {
        return config.getFloat(name, defaultValue);
    }

    @Override
    public double getDouble(String name, double defaultValue)
    {
        return config.getDouble(name, defaultValue);
    }

    @Override
    public boolean getBoolean(String name, boolean defaultValue)
    {
        return config.getBoolean(name, defaultValue);
    }

    @Override
    public <T extends Enum<T>> T getEnum(String name, T defaultValue)
    {
        return config.getEnum(name, defaultValue);
    }

    @Override
    public long getTimeDuration(String name, long defaultValue, TimeUnit unit)
    {
        return config.getTimeDuration(name, defaultValue, unit);
    }

    @Override
    public Pattern getPattern(String name, Pattern defaultValue)
    {
        return config.getPattern(name, defaultValue);
    }

    @Override
    public IntegerRanges getRange(String name, String defaultValue)
    {
        return config.getRange(name, defaultValue);
    }

    @Override
    public Collection<String> getStringCollection(String name)
    {
        return config.getStringCollection(name);
    }

    @Override
    public String[] getStrings(String name)
    {
        return config.getStrings(name);
    }

    @Override
    public String[] getStrings(String name, String... defaultValue)
    {
        return config.getStrings(name, defaultValue);
    }

    @Override
    public Collection<String> getTrimmedStringCollection(String name)
    {
        return config.getTrimmedStringCollection(name);
    }

    @Override
    public String[] getTrimmedStrings(String name)
    {
        return config.getTrimmedStrings(name);
    }

    @Override
    public String[] getTrimmedStrings(String name, String... defaultValue)
    {
        return config.getTrimmedStrings(name, defaultValue);
    }

    @Override
    public char[] getPassword(String name)
            throws IOException
    {
        return config.getPassword(name);
    }

    @Override
    public InetSocketAddress getSocketAddr(String hostProperty, String addressProperty, String defaultAddressValue, int defaultPort)
    {
        return config.getSocketAddr(hostProperty, addressProperty, defaultAddressValue, defaultPort);
    }

    @Override
    public InetSocketAddress getSocketAddr(String name, String defaultAddress, int defaultPort)
    {
        return config.getSocketAddr(name, defaultAddress, defaultPort);
    }

    @Override
    public Class<?> getClassByName(String name)
            throws ClassNotFoundException
    {
        return config.getClassByName(name);
    }

    @Override
    public Class<?> getClassByNameOrNull(String name)
    {
        return config.getClassByNameOrNull(name);
    }

    @Override
    public Class<?>[] getClasses(String name, Class<?>... defaultValue)
    {
        return config.getClasses(name, defaultValue);
    }

    @Override
    public Class<?> getClass(String name, Class<?> defaultValue)
    {
        return config.getClass(name, defaultValue);
    }

    @Override
    public <U> Class<? extends U> getClass(String name, Class<? extends U> defaultValue, Class<U> xface)
    {
        return config.getClass(name, defaultValue, xface);
    }

    @Override
    public <U> List<U> getInstances(String name, Class<U> xface)
    {
        return config.getInstances(name, xface);
    }

    @Override
    public Path getLocalPath(String dirsProp, String path)
            throws IOException
    {
        return config.getLocalPath(dirsProp, path);
    }

    @Override
    public File getFile(String dirsProp, String path)
            throws IOException
    {
        return config.getFile(dirsProp, path);
    }

    @Override
    public URL getResource(String name)
    {
        return config.getResource(name);
    }

    @Override
    public InputStream getConfResourceAsInputStream(String name)
    {
        return config.getConfResourceAsInputStream(name);
    }

    @Override
    public Reader getConfResourceAsReader(String name)
    {
        return config.getConfResourceAsReader(name);
    }

    @Override
    public Set<String> getFinalParameters()
    {
        return config.getFinalParameters();
    }

    @Override
    public int size()
    {
        return config.size();
    }

    @Override
    public void clear()
    {
        config.clear();
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator()
    {
        return config.iterator();
    }

    @Override
    public ClassLoader getClassLoader()
    {
        return config.getClassLoader();
    }

    @Override
    public void readFields(DataInput in)
            throws IOException
    {
        config.readFields(in);
    }

    @Override
    public void write(DataOutput out)
            throws IOException
    {
        config.write(out);
    }

    @Override
    public void writeXml(OutputStream out)
            throws IOException
    {
        config.writeXml(out);
    }

    @Override
    public void writeXml(Writer out)
            throws IOException
    {
        config.writeXml(out);
    }

    @Override
    public Map<String, String> getValByRegex(String regex)
    {
        return config.getValByRegex(regex);
    }

    @Override
    public String toString()
    {
        return config.toString();
    }

    @Override
    public void addResource(String name)
    {
        config.addResource(name);
    }

    @Override
    public void addResource(URL url)
    {
        config.addResource(url);
    }

    @Override
    public void addResource(Path file)
    {
        config.addResource(file);
    }

    @Override
    public void addResource(InputStream in)
    {
        config.addResource(in);
    }

    @Override
    public void addResource(InputStream in, String name)
    {
        config.addResource(in, name);
    }

    @Override
    public void addResource(Configuration conf)
    {
        config.addResource(conf);
    }

    @Override
    public void reloadConfiguration()
    {
        config.reloadConfiguration();
    }

    @Override
    public String[] getPropertySources(String name)
    {
        return config.getPropertySources(name);
    }

    private void checkAndSet(Runnable action)
    {
        if (isMutable) {
            action.run();
            return;
        }
        synchronized (lock) {
            if (!isMutable) {
                Configuration originalConfig = config;
                config = new Configuration(originalConfig);
                isMutable = true;
            }
            action.run();
        }
    }
}
