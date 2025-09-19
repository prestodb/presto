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
package com.facebook.presto.plugin.clp.split;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.plugin.clp.ClpConfig;
import com.facebook.presto.plugin.clp.ClpSplit;
import com.facebook.presto.plugin.clp.ClpTableHandle;
import com.facebook.presto.plugin.clp.ClpTableLayoutHandle;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.plugin.clp.ClpSplit.SplitType;
import static com.facebook.presto.plugin.clp.ClpSplit.SplitType.ARCHIVE;
import static com.facebook.presto.plugin.clp.ClpSplit.SplitType.IR;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ClpPinotSplitProvider
        implements ClpSplitProvider
{
    private static final Logger log = Logger.get(ClpPinotSplitProvider.class);
    private static final String SQL_SELECT_SPLITS_TEMPLATE = "SELECT tpath FROM %s WHERE 1 = 1 AND (%s) LIMIT 999999";
    private final ClpConfig config;

    @Inject
    public ClpPinotSplitProvider(ClpConfig config)
    {
        this.config = config;
    }

    @Override
    public List<ClpSplit> listSplits(ClpTableLayoutHandle clpTableLayoutHandle)
    {
        ImmutableList.Builder<ClpSplit> splits = new ImmutableList.Builder<>();
        ClpTableHandle clpTableHandle = clpTableLayoutHandle.getTable();
        String tableName = clpTableHandle.getSchemaTableName().getTableName();
        try {
            URL url = new URL(config.getMetadataDbUrl() + "/query/sql");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Accept", "application/json");
            conn.setDoOutput(true);
            conn.setConnectTimeout((int) SECONDS.toMillis(5));
            conn.setReadTimeout((int) SECONDS.toMillis(30));

            String query = format(SQL_SELECT_SPLITS_TEMPLATE, tableName, clpTableLayoutHandle.getMetadataSql().orElse("1 = 1"));
            log.info("Pinot query: %s", query);
            ObjectMapper mapper = new ObjectMapper();
            String body = format("{\"sql\": %s }", mapper.writeValueAsString(query));
            try (OutputStream os = conn.getOutputStream()) {
                os.write(body.getBytes(StandardCharsets.UTF_8));
            }

            int code = conn.getResponseCode();
            InputStream is = (code >= 200 && code < 300) ? conn.getInputStream() : conn.getErrorStream();
            if (is == null) {
                throw new IOException("Pinot HTTP " + code + " with empty body");
            }

            JsonNode root;
            try (InputStream in = is) {
                root = mapper.readTree(in);
            }
            JsonNode resultTable = root.get("resultTable");
            if (resultTable == null) {
                throw new RuntimeException("No \"resultTable\" field found");
            }
            JsonNode rows = resultTable.get("rows");
            if (rows == null) {
                throw new RuntimeException("No \"rows\" field found");
            }
            for (Iterator<JsonNode> it = rows.elements(); it.hasNext(); ) {
                JsonNode row = it.next();
                String splitPath = row.elements().next().asText();
                SplitType splitType = splitPath.endsWith(".clp.zst") ? IR : ARCHIVE;
                splits.add(new ClpSplit(splitPath, splitType, clpTableLayoutHandle.getKqlQuery()));
            }
            List<ClpSplit> filteredSplits = splits.build();
            log.debug("Number of filtered splits: %s", filteredSplits.size());
            return filteredSplits;
        }
        catch (Exception e) {
            log.error(e);
        }

        return Collections.emptyList();
    }
}
