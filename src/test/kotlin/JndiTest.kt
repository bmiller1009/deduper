import org.apache.commons.dbcp2.BasicDataSource
import org.bradfordmiller.deduper.Deduper
import org.bradfordmiller.deduper.config.Config
import org.bradfordmiller.deduper.jndi.JNDIUtils
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.osjava.sj.jndi.MemoryContext
import org.slf4j.LoggerFactory
import java.io.File
import java.nio.file.Files
import javax.naming.Context
import javax.naming.InitialContext

class JndiTest {
    @Test fun addNewJndiResource() {

        JNDIUtils.addJndiConnection(
            "BradTestJNDI",
            "default_ds_2",
            mapOf("targetName" to "src/test/resources/data/outputData/targetName")
        )

        /*val ds = BasicDataSource()

        ds.driverClassName = "org.sqlite.JDBC"
        ds.url = "jdbc:sqlite:src/test/resources/data/outputData/real_estate.db"

        val ctx = InitialContext() as Context

        //ctx.addToEnvironment("BradTestJNDI", ds)

        //ctx.close()
        //ctx.environment.



        val mc = (ctx.lookup("default_ds") as MemoryContext)

        //val list = mc.

        //mc.addToEnvironment("SqlLiteTesting", ds)

        val root = mc.environment.get("org.osjava.sj.root")

        mc.bind("SqlLiteTesting", ds)

        mc.close()*/
    }
}