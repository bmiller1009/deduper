package org.bradfordmiller.deduper.jndi

import org.osjava.sj.jndi.MemoryContext
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.SQLException
import javax.naming.Context
import javax.naming.InitialContext
import javax.naming.NamingException
import javax.sql.DataSource


class JNDIUtils {

    companion object {

        private val logger = LoggerFactory.getLogger(javaClass)

        fun getDataSource(jndi: String, context: String): DataSource? {
            val ctx = InitialContext() as Context
            val ds: DataSource? = try {
                (ctx.lookup(context) as MemoryContext).lookup(jndi) as DataSource
            } catch (nex: NamingException) {
                logger.error(nex.explanation)
                throw nex
            }
            return ds
        }

        fun getConnection(ds: DataSource): Connection? {
            val conn = try {ds.connection}
            catch(sqlEx: SQLException) {
                logger.error(sqlEx.message)
                throw sqlEx
            }
            return conn
        }
    }
}