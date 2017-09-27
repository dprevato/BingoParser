using FastMember;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.IO;
using static BingoParser.ParseArguments;
using static BingoParser.Program;

namespace BingoParser
{
  public static class BulkImport
  {
    // Qui si esegue la preimportazione in Giusto!2010
    public static string SetDbConnection() {
#if DEBUG
      return ConfigurationManager.ConnectionStrings["LocalConnection"].ConnectionString;
#else
      return ConfigurationManager.ConnectionStrings["ProductionConnection"].ConnectionString;
#endif
    }

    static IEnumerable<string> ReadTsvLines(string tsvFile) {
      using (var reader = File.OpenText(tsvFile)) {
        var line = "";
        while ((line = reader.ReadLine()) != null) yield return line;
      }
    }

    static void PerformBulkCopyToSqlServer() {
      using (var bcp = new SqlBulkCopy(SetDbConnection()))
      using (var reader = ObjectReader.Create(ReadTsvLines(TsvFileName), HeaderData)) {
        bcp.DestinationTableName = "Misure.RawImportData";
        bcp.WriteToServer(reader);
      }
    }

    public static void WriteAllInDatabase() {
      var parser = new GenericParsing.GenericParserAdapter(TsvFileName) { ColumnDelimiter = Separator[0], FirstRowHasHeader = true };
      var dt = parser.GetDataTable();
      using (var bcp = new SqlBulkCopy(SetDbConnection())) {
        bcp.DestinationTableName = "Misure.RawImportData";
        bcp.NotifyAfter = 1000;
        bcp.SqlRowsCopied += ShowBulkCopyActivity;
        bcp.WriteToServer(dt);
      }
    }

    static void ShowBulkCopyActivity(Object sender, SqlRowsCopiedEventArgs e) {
      if (Quiet) return;
      UpdateConsole("Tabella RawImportData", e.RowsCopied);
    }
  }
}


