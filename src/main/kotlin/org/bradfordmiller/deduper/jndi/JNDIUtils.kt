package org.bradfordmiller.deduper.jndi

import org.apache.commons.io.FilenameUtils
import org.bradfordmiller.deduper.utils.Either
import org.bradfordmiller.deduper.utils.Left
import org.bradfordmiller.deduper.utils.Right
import org.osjava.sj.jndi.MemoryContext
import org.slf4j.LoggerFactory
import java.io.*
import java.nio.file.Files
import java.nio.file.Paths
import java.rmi.activation.UnknownObjectException
import java.sql.Connection
import java.sql.SQLException
import java.util.*
import javax.naming.*
import javax.sql.DataSource
import kotlin.streams.toList

class JNDIUtils {

    companion object {

        private val logger = LoggerFactory.getLogger(JNDIUtils::class.java)

        fun getMemoryContextFromInitContext(initCtx: InitialContext, contextName: String): MemoryContext? {
            return try {
                val mc = (initCtx.lookup(contextName) as MemoryContext)
                mc
            } catch (ne: NamingException) {
                logger.info("Naming exception occurred on jndi lookup of context $contextName: ${ne.message}")
                null
            }
        }
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
                logger.error("Error fetching connection from data source: ${sqlEx.message}")
                throw sqlEx
            }
        }
        fun getJndiConnection(jndiString: String, context: String): Connection {
            val jndi = (getDataSource(jndiString, context) as Left<DataSource?, String>).left!!
            return jndi.connection
        }
        fun getAvailableJndiContexts(): List<String> {
            val ctx = InitialContext()
            val root = ctx.environment.get("org.osjava.sj.root").toString()

            try {
                val files = Files.walk(Paths.get(root)).filter {it ->
                    val ext = FilenameUtils.getExtension(it.fileName.toString())
                    !(Files.isDirectory(it)) && ext == "properties"
                }
                return files.map {it.toString()}.toList()
            } catch(ioEx: IOException) {
                logger.error("Error listing jndi contexts: ${ioEx.message}")
                throw ioEx
            }
        }
        fun getEntriesForJndiContext(memoryContext: MemoryContext): Map<String, String> {
            val field = memoryContext.javaClass.getDeclaredField("namesToObjects")
            field.isAccessible = true

            val fieldMap = field.get(memoryContext) as Map<Name, Object>

            val map = fieldMap.map {
                it.key.toString() to it.value.toString()
            }.toMap()

            return map
        }
        fun getDetailsforJndiEntry(context: InitialContext, jndiName: String, entry: String): Pair<String, String> {
            val mc = getMemoryContextFromInitContext(context, jndiName)
            val entries = getEntriesForJndiContext(mc!!)
            return entry to entries[entry]!!
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

            val ctx = InitialContext()

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

                    val memoryContext = getMemoryContextFromInitContext(ctx, context)

                    if (memoryContext != null) {
                        //Check if the key being added already exists
                        val fieldKeys = getEntriesForJndiContext(memoryContext)

                        if (fieldKeys.contains(jndiName)) {
                            val errorString = "Jndi name $jndiName already exists for context $context."
                            logger.error(errorString)
                            throw IllegalAccessException(errorString)
                        } else {
                            val uuid = UUID.randomUUID().toString()
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