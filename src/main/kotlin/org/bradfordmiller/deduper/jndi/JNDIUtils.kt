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
import javax.sql.DataSource

class JNDIUtils {

    companion object {

        private val logger = LoggerFactory.getLogger(JNDIUtils::class.java)

        fun getDataSource(jndi: String, context: String): Either<DataSource?, Map<*, *>> {
            val ctx = InitialContext() as Context
            val mc = (ctx.lookup(context) as MemoryContext)

            return when (val lookup = mc.lookup(jndi)) {
                is DataSource -> Left(lookup)
                is Map<*, *> -> Right(lookup)
                else -> {
                    throw UnknownObjectException(
                        "jndi entry $jndi for context $context is neither an DataSource or a Map<String, String>"
                    )
                }
            }
        }
        fun getConnection(ds: DataSource): Connection? {
            return try {
                ds.connection
            } catch(sqlEx: SQLException) {
                logger.error(sqlEx.message)
                throw sqlEx
            }
        }
        fun getJndiConnection(jndiString: String, context: String): Connection {
            val jndi = (getDataSource(jndiString, context) as Left<DataSource?, String>).left!!
            return jndi.connection
        }
    }
}