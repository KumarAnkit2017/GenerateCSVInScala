import com.gitbub.ka1904787.TestCase.TestCases
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.funsuite.AnyFunSuite

class ScalaTestCase extends AnyFunSuite{

  test("My First Test Case")
  {
    assert(2==TestCases.divideBy(20,10))// Test Passed
    assert(4==TestCases.divideBy(400,100))// Test Passed
  }











}
