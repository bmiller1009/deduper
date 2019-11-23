import org.bradfordmiller.deduper.jndi.JNDIUtils
import org.bradfordmiller.deduper.utils.Left
import org.bradfordmiller.deduper.utils.Right
import org.junit.jupiter.api.BeforeAll

import org.junit.jupiter.api.Test
import org.osjava.sj.jndi.JndiUtils
import java.io.File
import javax.naming.Context
import javax.naming.InitialContext
import javax.sql.DataSource
import javax.xml.crypto.Data

class JndiTest {

    companion object {
        @BeforeAll
        @JvmStatic
        fun removeContext() {
            val ctx = "default_ds_3"
            val initCtx = InitialContext()
            val root = initCtx.environment.get("org.osjava.sj.root").toString()
            val file = "$root/$ctx.properties"
            if (File(file).exists())
                File(file).delete()
            initCtx.close()
        }
    }

    @Test
    fun addNewJndiResources() {

        val initialContext = InitialContext()
        val jndi = "BradTestJNDI"
        val ctx = "default_ds_3"
        val root = initialContext.environment.get("org.osjava.sj.root").toString()

        JNDIUtils.addJndiConnection(
            jndi,
            ctx,
            mapOf(
                "type" to "java.util.Map",
                "targetName" to "src/test/resources/data/outputData/ds3"
            )
        )

        JNDIUtils.addJndiConnection(
            "BradTestJNDI_22",
            ctx,
            mapOf(
                "type" to "java.util.Map",
                "targetName" to "src/test/resources/data/outputData/testJNDI"
            )
        )

        JNDIUtils.addJndiConnection(
            "BradTestJNDI_23",
            ctx,
            mapOf(
                "type" to "javax.sql.DataSource",
                "driver" to "org.sqlite.JDBC",
                "url" to "jdbc:sqlite:src/test/resources/data/outputData/real_estate.db",
                "user" to "test_user",
                "password" to "test_password"
            )
        )

        val entryAddNew = (JNDIUtils.getDataSource(jndi, ctx) as Right<DataSource?, Map<String, String>>).right
        val entryAddExistingCsv =
            (JNDIUtils.getDataSource("BradTestJNDI_22", ctx) as Right<DataSource?, Map<String, String>>).right
        val entryAddExistingSql =
            (JNDIUtils.getDataSource("BradTestJNDI_23", ctx) as Left<DataSource?, Map<String, String>>).left

        assert((entryAddNew is Map<String, String>))
        assert((entryAddExistingCsv is Map<String, String>))
        assert((entryAddExistingSql is DataSource?))

        File("$root/$ctx.properties").delete()
    }

    @Test
    fun getAllJndiContexts() {
        val contexts = JNDIUtils.getAvailableJndiContexts()
        assert(contexts.size == 1)
        assert(contexts[0] == "src/test/resources/jndi/default_ds.properties")
    }

    @Test
    fun getEntriesForJndiContext() {
        val initCtx = InitialContext()
        val contextName = "default_ds"
        val mc = JNDIUtils.getMemoryContextFromInitContext(initCtx, contextName)
        val entries = JNDIUtils.getEntriesForJndiContext(mc!!)

        assert(mc != null)
        assert(entries.size == 14)
        assert(entries.get("RealEstateOutDupesUseDefaults") == "{targetName=src/test/resources/data/outputData/dupeName}")
    }

    @Test
    fun getJndiEntry() {
        val initCtx = InitialContext()
        val contextName = "default_ds"
        val entry = "SqlLiteTestPW"
        val entryResult = JNDIUtils.getDetailsforJndiEntry(initCtx, contextName, entry)

        assert(entryResult.first == "SqlLiteTestPW")
        assert(entryResult.second == "org.sqlite.JDBC::::jdbc:sqlite:src/test/resources/data/outputData/real_estate.db::::TestUser")
    }
}