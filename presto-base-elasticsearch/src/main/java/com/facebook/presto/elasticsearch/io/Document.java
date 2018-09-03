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
package com.facebook.presto.elasticsearch.io;

import java.util.Map;

public class Document
{
    private String index;
    private String type = "presto";
    private String id;

    private Map<String, Object> source;

    public Map<String, Object> getSource()
    {
        return source;
    }

    public String getId()
    {
        return id;
    }

    public String getIndex()
    {
        return index;
    }

    public String getType()
    {
        return type;
    }

    public static DocumentBuilder newDocument()
    {
        return new DocumentBuilder();
    }

    public static class DocumentBuilder
    {
        private final Document document = new Document();

        public DocumentBuilder setIndex(String index)
        {
            document.index = index;
            return this;
        }

        public DocumentBuilder setType(String type)
        {
            document.type = type;
            return this;
        }

        public DocumentBuilder setId(String id)
        {
            document.id = id;
            return this;
        }

        public DocumentBuilder setSource(Map<String, Object> source)
        {
            document.source = source;
            return this;
        }

        public Document get()
        {
            return document;
        }
    }
}
