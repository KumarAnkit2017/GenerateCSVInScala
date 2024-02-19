
import com.gitbub.ka1904787.TestCase.TestCases
import junit.framework.TestCase
import junit.framework.TestCase.assertEquals
import org.junit.Test

class JUnitTestCase {

  @Test
  def test1(): Unit = {
    val accepted:Int=2 //3
    val actual:Int=TestCases.divideBy(20,10);
    assertEquals(accepted,actual)
  }

}
