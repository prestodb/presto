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

import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * presto-root
 *
 * @author guodong
 */
public class JDBCDriverTest
{
    @Test
    public void testJDBC()
    {
        try {
            Properties props = new Properties();
            props.setProperty("user", "jelly");
            props.setProperty("password", "jelly");
            Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/metabase", props);
            PreparedStatement statement = connection.prepareStatement("select name, db_name from tbls where name='student'");
            ResultSet resultSet = statement.executeQuery();
            while (resultSet != null && resultSet.next()) {
                System.out.println(resultSet.getString("db_name"));
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
