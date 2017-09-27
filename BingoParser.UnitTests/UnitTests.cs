using NUnit.Framework;
using static BingoParser.Program;

namespace BingoParser.UnitTests
{
  [TestFixture]
  public static class UnitTests
  {
    [Test]
    public static void FormatDataForSql_WithDateAndTime_ReturnsCorrectDateTime() {
      var testDate = "24/09/2017";
      var testTime = "11:33:00";

      var retval = FormatDateForSql(testDate, testTime);
      Assert.AreEqual(retval, "2017-09-24 11:33");
    }

    [Test]
    public static void FormatDataForSql_WithDateTime_ReturnsCorrectDateTime() {
      var testDate = "201709241143";

      var retval = FormatDateForSql(testDate);
      Assert.AreEqual(retval, "2017-09-24 11:43");
    }

    [Test]
    public static void GetCleanDataRow_WithDirtString_ReturnsCleanString() {
      const string testString = "26/01/2005\t  10:04:42\t****\t-99999\t3,14\t-99999\t  2,718\t****\t200501261004\t";
      const string expected = "26/01/2005\t10:04:42\t-999\t-999\t3.14\t-999\t2.718\t-999\t200501261004";
      var retval = GetCleanDataRow(testString);
      Assert.AreEqual(retval, expected);
    }
  }
}
