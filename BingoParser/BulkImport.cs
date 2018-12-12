using System.Data.SqlClient;
using System.IO;
using Anotar.NLog;
using LumenWorks.Framework.IO.Csv;
using static BingoParser.ParseArguments;
using static BingoParser.Program;

namespace BingoParser
{
   public static class BulkImport
   {
      // Qui si esegue la preimportazione in Giusto!2010
      public static string GetDbConnection() {
#if DEBUG
         return @"Server=Gandalf\SQLEXPRESS;Database=G2009-test;Trusted_Connection=yes;";
#else
         return ConfigurationManager.ConnectionStrings["DbConnection"].ConnectionString;
#endif
      }

      public static void WriteAllTsvToServer() {
         RowsWritten = 0;
         LogTo.Info($"Inizio Preimportazione.");
         DeleteBulkImportTable();
         WriteSingleTsvToServer($"{OutputDirectory}{OutputFile}");
         UpdateConsole("\nNumero totale di righe trasferite", TotalRowsWritten);
         LogTo.Info($"Preimportazione terminata.");
      }

      private static void DeleteBulkImportTable() {
         using (var connection = new SqlConnection(GetDbConnection())) {
            var cmd = new SqlCommand($"TRUNCATE TABLE {BulkImportTableName};", connection);
            connection.Open();
            cmd.ExecuteNonQuery();
         }
         LogTo.Trace(@"Vuotata la tabella di preimportazione.");
      }

      [LogToErrorOnException]
      private static void WriteSingleTsvToServer(string file) {
         using (var sr = new StreamReader(file))
         using (var dt = new CsvReader(sr, true, Separator[0]))
         using (var bcp = new SqlBulkCopy(GetDbConnection()))
         {
            bcp.DestinationTableName = "Misure.RawImportData";
            bcp.NotifyAfter = 10000;
            bcp.SqlRowsCopied += ShowBulkCopyActivity;
            bcp.BulkCopyTimeout = 3600;
            bcp.WriteToServer(dt);
         }
      }

      private static void ShowBulkCopyActivity(object sender, SqlRowsCopiedEventArgs e) {
         if (Quiet) return;
         UpdateConsole("Preimportazione", RowsWritten + e.RowsCopied);
      }
   }
}