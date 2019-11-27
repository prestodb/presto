/**
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
package com.victoriametrics.presto

import com.github.ajalt.clikt.core.CliktCommand
import java.nio.file.FileSystems
import java.sql.DriverManager

class Main : CliktCommand() {
    private val fs = FileSystems.getDefault()!!
    override fun run() {
        connectToVm()
    }

    private fun connectToVm() {

    }

    // TODO: move to integration tests
    private fun connectToPresto() {
        val url = "jdbc:presto://localhost:8080/jmx/current"
        val connection = DriverManager.getConnection(url, "test", null)
        val statement = connection.createStatement()
        val result = statement.executeQuery("SHOW TABLES")
        result.next()
        print(result.getString(1))
        statement.close()
    }
}

fun main(args: Array<String>) {
    Main().main(args)
}


