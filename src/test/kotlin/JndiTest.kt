import org.bradfordmiller.deduper.jndi.JNDIUtils

import org.junit.jupiter.api.Test

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
}