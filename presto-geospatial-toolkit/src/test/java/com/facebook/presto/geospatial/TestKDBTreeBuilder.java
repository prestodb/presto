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
package com.facebook.presto.geospatial;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.json.ObjectMapperProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.assertEquals;

public class TestKDBTreeBuilder
{
    @Test
    public void test()
            throws IOException
    {
        int totalCount = 128_407_084;
        String filePath = "/Users/mbasmanova/Downloads/daiquery-2051178054910468-916678561846213-2018-03-23T090611-0700.tsv";
        Envelope extent = new Envelope(-180, -90, 180, 90);

        KDBTreeBuilder kdbTreeBuilder = new KDBTreeBuilder(10_000, 100, extent);

        List<String> lines = Files.readAllLines(Paths.get(filePath));
        for (String wkt : lines.subList(1, lines.size())) {
            OGCGeometry geometry = OGCGeometry.fromText(wkt);
            Envelope envelope = new Envelope();
            geometry.getEsriGeometry().queryEnvelope(envelope);

            kdbTreeBuilder.insert(envelope);
        }

        KDBTreeSpec spec = kdbTreeBuilder.build();
        // -72.67913, 40.775191, -43.34359, 42.555235
        Map<Integer, Envelope2D> leafs = KDBTree.fromSpec(spec).findLeafs(new Envelope2D(-72.67913, 40.775191, -72.67913, 40.775191));
        assertEquals(leafs.size(), 3);
        System.out.println(leafs.keySet());
        System.out.println(leafs.entrySet());
        Set<Integer> ids = KDBTree.fromSpec(spec).findLeafs(new Envelope2D(10, -71, 41, -70)).keySet();
        System.out.println(ids);

        ObjectMapper objectMapper = new ObjectMapperProvider().get();
        String json = objectMapper.writer().writeValueAsString(spec);
        System.out.println(json);

        KDBTreeSpec reconstructedSpec = objectMapper.readValue(json, KDBTreeSpec.class);
        KDBTree reconstructedTree = KDBTree.fromSpec(reconstructedSpec);
        Set<Integer> ids2 = reconstructedTree.findLeafs(new Envelope2D(10, -71, 41, -70)).keySet();
        System.out.println(ids2);
    }
}
