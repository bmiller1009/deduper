package org.bradfordmiller.deduper.jndi

import org.bradfordmiller.deduper.utils.Either
import org.bradfordmiller.deduper.utils.Left
import org.bradfordmiller.deduper.utils.Right
import org.osjava.sj.jndi.MemoryContext
import org.slf4j.LoggerFactory
import java.io.*
import java.rmi.activation.UnknownObjectException
import java.sql.Connection
import java.sql.SQLException
import javax.naming.Context
import javax.naming.InitialContext
import javax.naming.NamingException
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
        //TODO: Make sure to put a lock file in place while updating the jndi root directory
        fun addJndiConnection(jndiName: String, context: String, values: Map<String, String>) {

            val ctx = InitialContext() as Context

            val root = ctx.environment.get("org.osjava.sj.root").toString()
            val colonReplace = ctx.environment.get("org.osjava.sj.colon.replace")
            val delimiter = ctx.environment.get("org.osjava.sj.delimiter")

            val memoryContext =
                try {
                    val mc = (ctx.lookup(context) as MemoryContext)
                    mc
                } catch(ne: NamingException) {
                    logger.info("Naming exception occurred on jndi lookup of context $context: ${ne.message}")
                    null
                }

            //context was found, so append to it
            if(memoryContext != null) {
                //Copy the context file
                //Append the data to the copy
                //Rename the copy to the original
                //Delete the old file


            } else {

                val file = File(root + "/$context.properties")
                val fileCreated = file.createNewFile()
                FileOutputStream(file).use { fos ->
                    val lock = fos.channel.lock()
                    try {
                        val filePath = file.toRelativeString()
                        BufferedWriter(FileWriter(filePath)).use { bw ->
                            values.forEach {
                                val s = jndiName + delimiter + it.key + "=" + it.value
                                bw.write(s)
                            }
                        }
                    } finally {
                        lock.release()
                    }
                }
            }




        }
    }
}