import org.bradfordmiller.deduper.jndi.JNDIUtils

import org.junit.jupiter.api.Test
import org.osjava.sj.jndi.JndiUtils

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

    @Test fun getAllJndiContexts() {
        val contexts = JNDIUtils.getAvailableJndiContexts()
        println(contexts)
    }
}