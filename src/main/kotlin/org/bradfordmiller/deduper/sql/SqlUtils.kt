package org.bradfordmiller.deduper.sql

import org.slf4j.LoggerFactory
import java.sql.ResultSet
import java.sql.ResultSetMetaData

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
    }
}