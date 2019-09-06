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
import java.util.*
import javax.naming.*
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
            } catch (sqlEx: SQLException) {
                logger.error(sqlEx.message)
                throw sqlEx
            }
        }

        fun getJndiConnection(jndiString: String, context: String): Connection {
            val jndi = (getDataSource(jndiString, context) as Left<DataSource?, String>).left!!
            return jndi.connection
        }

        //TODO: Make sure to put a lock file in place while updating the jndi root directory
        fun addJndiConnection(jndiName: String, context: String, values: Map<String, String>): Boolean {

            fun writeToFile(backupFile: File, delimiter: String) {
                BufferedWriter(FileWriter(backupFile.absolutePath, true)).use { bw ->
                    bw.write("\n")
                    values.forEach {
                        val s = jndiName + delimiter + it.key + "=" + it.value
                        bw.write("$s\n")
                    }
                }
            }

            val ctx = InitialContext() as Context

            val root = ctx.environment.get("org.osjava.sj.root").toString()
            val colonReplace = ctx.environment.get("org.osjava.sj.colon.replace")
            val delimiter = ctx.environment.get("org.osjava.sj.delimiter").toString()
            val file = File("$root/$context.properties")
            val lockFile = File("$root/.LOCK")

            return try {

                if (lockFile.exists()) {
                    logger.info("jndi director is currently being modified. Please try to add your new entry later.")
                    false
                } else {
                    logger.info("Lock file is absent. Locking directory before proceeding")
                    lockFile.createNewFile()

                    val memoryContext =
                            try {
                                val mc = (ctx.lookup(context) as MemoryContext)
                                mc
                            } catch (ne: NamingException) {
                                logger.info("Naming exception occurred on jndi lookup of context $context: ${ne.message}")
                                null
                            }

                    if (memoryContext != null) {
                        //Check if the key being added already exists
                        val field = memoryContext.javaClass.getDeclaredField("namesToObjects")
                        field.isAccessible = true

                        val fieldMap = field.get(memoryContext) as Map<Name, Object>
                        val fieldKeys = fieldMap.keys.map { k -> k.toString() }

                        if (fieldKeys.contains(jndiName)) {
                            val errorString = "Jndi name $jndiName already exists for context $context."
                            logger.error(errorString)
                            throw IllegalAccessException(errorString)
                        } else {
                            val uuid = UUID.randomUUID().toString()
                            val ap = file.absolutePath
                            val backupFile = File(file.parentFile.name + "/" + context + "_" + uuid + ".properties")
                            file.copyTo(backupFile, true)
                            //Append the data to the copy
                            writeToFile(backupFile, delimiter)
                            //Rename the copy to the original
                            backupFile.renameTo(file)
                            true
                        }

                    } else {
                        val f = File("$root/$context.properties")
                        //Write the data to the copy
                        writeToFile(f, delimiter)
                        true
                    }
                }
            } finally {
                logger.info("Deleting lock file. Directory is writable.")
                lockFile.delete()
            }
        }
    }
}
