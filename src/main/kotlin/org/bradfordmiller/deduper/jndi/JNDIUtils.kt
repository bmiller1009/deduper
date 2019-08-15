package org.bradfordmiller.deduper.jndi

import org.bradfordmiller.deduper.utils.Either
import org.bradfordmiller.deduper.utils.Left
import org.bradfordmiller.deduper.utils.Right
import org.osjava.sj.jndi.MemoryContext
import org.slf4j.LoggerFactory
import java.rmi.activation.UnknownObjectException
import java.sql.Connection
import java.sql.SQLException
import javax.naming.Context
import javax.naming.InitialContext
import javax.naming.NamingException
import javax.sql.DataSource

data class FileStore(val fileName: String, val ext: String, val delimiter: String)

class JNDIUtils {

    companion object {

        private val logger = LoggerFactory.getLogger(javaClass)

        fun getDataSource(jndi: String, context: String): Either<DataSource?, Map<*, *>> {
            val ctx = InitialContext() as Context
            val mc = (ctx.lookup(context) as MemoryContext)
            val lookup = mc.lookup(jndi)

            return when (lookup) {
                is DataSource -> Left(lookup)
                is Map<*, *> -> Right(lookup)
                else -> {
                    throw UnknownObjectException("jndi entry $jndi for context $context is neither an DataSource or a Map<String, String>")
                }
            }
        }

        fun getConnection(ds: DataSource): Connection? {
            val conn = try {ds.connection}
            catch(sqlEx: SQLException) {
                logger.error(sqlEx.message)
                throw sqlEx
            }
            return conn
        }

        fun getJndiConnection(jndi: String, context: String): Connection {
            val jndi = (getDataSource(jndi, context) as Left<DataSource?, String>).left!!
            return jndi.connection
        }
    }
}