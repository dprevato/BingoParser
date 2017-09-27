using GenericParsing;
using System;
using System.Configuration;
using System.Data;
using System.Data.OleDb;
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

    public static long RecordsWritten { get; set; }

    public static void WriteAllTsvToServer() {
      RecordsWritten = 0;
      foreach (var file in Directory.GetFiles(OutputDirectory, "*.tsv")) WriteSingleTsvToServer(file);
      UpdateConsole("Numro totale di righe trasferite", RecordsWritten);
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

    static void TestWriteSingleTsvToServer(string file) {
      using (var parser = new GenericParserAdapter(file) { ColumnDelimiter = Separator[0], FirstRowHasHeader = true }) {
        var dt = parser.GetDataTable();
        var connection = new OleDbConnection(SetDbConnection());
        connection.Open();
        var InsertCommand = new OleDbCommand { CommandType = CommandType.Text, Connection = connection };
        foreach (DataRow dataRow in dt.Rows) {
          var cmdText = "INSERT INTO Misure.RawImportData (SourceTable, SourceFilter, GaugeDateTime, GaugeText, GaugeValue, GaugeTaken) VALUES (";
          cmdText = $"{cmdText}'{dataRow[0]}', '{dataRow[1]}', '{dataRow[2]}', '{dataRow[3]}', {dataRow[4]}, '{dataRow[5]}')";
          InsertCommand.CommandText = cmdText;
          InsertCommand.ExecuteNonQuery();
        }
        connection.Close();
      }
    }

    static void ShowBulkCopyActivity(Object sender, SqlRowsCopiedEventArgs e) {
      if (Quiet) return;
      UpdateConsole("Tabella RawImportData", e.RowsCopied);
    }
  }
}