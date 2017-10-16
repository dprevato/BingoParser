using GenericParsing;
using System;
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
      return @"Server=Gandalf\SQLEXPRESS;Database=G2009P;Trusted_Connection=yes;";
      // return ConfigurationManager.ConnectionStrings["DbConnection"].ConnectionString;
    }

    public static long RecordsWritten { get; set; }

    public static void WriteAllTsvToServer() {
      RecordsWritten = 0;
      foreach (var file in Directory.GetFiles(OutputDirectory, "*.tsv")) WriteSingleTsvToServer(file);
      UpdateConsole("Numero totale di righe trasferite", RecordsWritten);
    }

    static void WriteSingleTsvToServer(string file) {
      using (var parser = new GenericParserAdapter(file) { ColumnDelimiter = Separator[0], FirstRowHasHeader = true })
      using (var bcp = new SqlBulkCopy(SetDbConnection())) {
        var dt = parser.GetDataTable();
        bcp.DestinationTableName = "Misure.RawImportData";
        bcp.NotifyAfter = 1;
        bcp.SqlRowsCopied += ShowBulkCopyActivity;
        bcp.BulkCopyTimeout = 600;
        bcp.WriteToServer(dt);
        RecordsWritten += dt.Rows.Count;
      }
    }

    static void ShowBulkCopyActivity(Object sender, SqlRowsCopiedEventArgs e) {
      if (Quiet) return;
      UpdateConsole("Tabella RawImportData", e.RowsCopied);
    }
  }
}