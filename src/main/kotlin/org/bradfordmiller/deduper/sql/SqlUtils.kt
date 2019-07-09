package org.bradfordmiller.deduper.sql

import org.apache.ddlutils.PlatformFactory
import org.bradfordmiller.deduper.jndi.JNDIUtils
import org.slf4j.LoggerFactory
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

        fun createOutputTable(
                sourceDs: DataSource,
                sourceCatalog: String,
                sourceSchema: String,
                sourceTable: String,
                targetDs: DataSource,
                targetCatalog: String,
                targetSchema: String,
                targetTable: String
        ) {
            val platform = PlatformFactory.createNewPlatformInstance(sourceDs)
            val db = platform.readModelFromDatabase("model")

        }
    }
}