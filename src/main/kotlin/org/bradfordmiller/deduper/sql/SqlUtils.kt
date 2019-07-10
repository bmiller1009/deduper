package org.bradfordmiller.deduper.sql


import org.bradfordmiller.deduper.Deduper
import org.bradfordmiller.deduper.jndi.JNDIUtils
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.JDBCType
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import javax.sql.DataSource

class SqlUtils {
    companion object {

        private val logger = LoggerFactory.getLogger(javaClass)

        fun getColumnsFromRs(rsmd: ResultSetMetaData): Set<String> {

            val colCount = rsmd.columnCount

            val rsColumns = (1 until colCount).toList().map {i ->
                rsmd.getColumnName(i)
            }.toSet()

            return rsColumns
        }

        fun generateDDL(tableName: String, rsmd: ResultSetMetaData): String {

            val ctasClause = "CREATE TABLE $tableName AS "

            val colCount = rsmd.columnCount

            val columns = (1 until colCount).map { c ->

                val colName = rsmd.getColumnName(c)
                val type = rsmd.getColumnType(c)
                val typeName = JDBCType.valueOf(type).name
                val size = rsmd.getColumnDisplaySize(c)

                colName + " " +
                        if (type == java.sql.Types.VARCHAR) {
                            typeName + " (" + size.toString() + ")"
                        } else {
                            typeName
                        }
            }

            return ctasClause + columns.joinToString(",")
        }

        fun executeDDL(conn: Connection, ddl: String) {
            conn.createStatement().use {stmt ->
                stmt.executeUpdate(ddl)
            }
        }
    }
}