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
package com.facebook.presto.hdfs.metaserver;

import com.facebook.presto.hdfs.HDFSConfig;

import java.sql.SQLException;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class TestJDBCMetaServer
{
    private HDFSConfig config = new HDFSConfig();

    public void testInit() throws SQLException
    {
        config.setJdbcDriver("org.postgresql.Driver");
        config.setMetaserverUri("jdbc:postgresql://127.0.0.1:5432/metaserver");
        config.setMetaserverUser("jelly");
        config.setMetaserverPass("jelly");

//        JDBCMetaServer metaServer = new JDBCMetaServer(config);
    }
}
