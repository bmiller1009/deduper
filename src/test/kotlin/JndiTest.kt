import org.bradfordmiller.deduper.jndi.JNDIUtils

import org.junit.jupiter.api.Test
import org.osjava.sj.jndi.JndiUtils
import javax.naming.InitialContext

class JndiTest {
    @Test fun addNewJndiResource() {

        JNDIUtils.addJndiConnection(
            "BradTestJNDI",
            "default_ds_3",
            mapOf(
                    "type" to "java.util.Map",
                    "targetName" to "src/test/resources/data/outputData/ds3"
                    )
        )
    }

    @Test fun addNewEntrytoExistingJndi() {
        JNDIUtils.addJndiConnection(
                "BradTestJNDI_22",
                "default_ds",
                mapOf(
                        "type" to "java.util.Map",
                        "targetName" to "src/test/resources/data/outputData/testJNDI"
                )
        )
    }

    @Test fun addNewSqlEntrytoExistingJndi() {
        JNDIUtils.addJndiConnection(
                "BradTestJNDI_23",
                "default_ds",
                mapOf(
                        "type" to "javax.sql.DataSource",
                        "driver" to "org.sqlite.JDBC",
                        "url" to "jdbc:sqlite:src/test/resources/data/outputData/real_estate.db",
                        "user" to "test_user",
                        "password" to "test_password"
                )
        )
    }

    @Test fun getAllJndiContexts() {
        val contexts = JNDIUtils.getAvailableJndiContexts()
        println(contexts)
    }

    @Test fun getEntriesForJndiContext() {
        val initCtx = InitialContext()
        val contextName = "default_ds"
        val mc = JNDIUtils.getMemoryContextFromInitContext(initCtx, contextName)
        if(mc != null) {
            val entries = JNDIUtils.getEntriesForJndiContext(mc)
            println(entries)
        }
    }
    @Test fun getJndiEntry() {
        val initCtx = InitialContext()
        val contextName = "default_ds"
        val entry = "SqlLiteTestPW"
        val entryResult = JNDIUtils.getDetailsforJndiEntry(initCtx, contextName, entry)
        println(entryResult)
    }
}