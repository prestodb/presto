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
package com.facebook.presto.kafkastream;

import com.google.common.base.Throwables;
import com.google.common.io.Resources;
import io.airlift.configuration.Config;

import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;

import static java.nio.charset.StandardCharsets.UTF_8;

// TODO: Need to contact Analytics Provisioning Service to get the config
public class KafkaConfig
{
    private static final Charset ENCODING = UTF_8;
    private static final String DEFAULT_URL = "/metadata.json";
    private URI configMetadataUri;
    private boolean classpath;
    private URL finalMetadataUrl;

    public String getMetadata()
    {
        try {
            return Resources.toString(getMetadataUrl(), ENCODING);
        }
        catch (Exception exp) {
            throw Throwables.propagate(exp);
        }
    }

    public String getMetadataUri()
    {
        try {
            return Resources.toString(getMetadataUrl(), ENCODING);
        }
        catch (Exception exp) {
            throw Throwables.propagate(exp);
        }
    }

    @Config("metadata-uri")
    public KafkaConfig setMetadataUri(URI metadataUri)
    {
        this.configMetadataUri = metadataUri;
        return this;
    }

    public boolean isClasspath()
    {
        return classpath;
    }

    @Config("classpath")
    public KafkaConfig setClasspath(boolean classpath)
    {
        this.classpath = classpath;
        return this;
    }

    public URL getMetadataUrl()
    {
        try {
            if (finalMetadataUrl == null) {
                finalMetadataUrl = (configMetadataUri == null) ? getResource(DEFAULT_URL)
                        : (classpath) ? getResource(configMetadataUri.toString()) : configMetadataUri.toURL();
            }
            return finalMetadataUrl;
        }
        catch (Exception exp) {
            throw Throwables.propagate(exp);
        }
    }

    private URL getResource(String resource)
    {
        return Resources.getResource(KafkaConfig.class, resource);
    }
}
