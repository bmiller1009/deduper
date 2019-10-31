import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.io.File
import java.util.*

class BuildTest {
    @Test fun gradleBuildTest() {
        val props = Properties()
        val inputStream = File("version.properties").inputStream()
        props.load(inputStream)
        val softwareVersion = props.get("version")!!.toString()
        println(softwareVersion)
    }
}