using GenericParsing;
using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Threading;
using Anotar.NLog;
using FileHelpers;
using static BingoParser.ParseArguments;
using static BingoParser.Program;

namespace BingoParser
{
   public static class BulkImport
   {
      // Qui si esegue la preimportazione in Giusto!2010
      public static string GetDbConnection() {
         return @"Server=Gandalf\SQLEXPRESS;Database=G2009-test;Trusted_Connection=yes;";
         //return ConfigurationManager.ConnectionStrings["DbConnection"].ConnectionString;
      }

      public static void WriteAllTsvToServer() {
         RowsWritten = 0;
         foreach (var file in Directory.GetFiles(OutputDirectory, "*.tsv")) WriteSingleTsvToServer(file);

         UpdateConsole("Numero totale di righe trasferite", RowsWritten);
         LogTo.Trace($"Preimportazione terminata. Scritte in totale {RowsWritten} righe.");
      }

      [LogToErrorOnException]
      private static void WriteSingleTsvToServer(string file) {
         var dt = new DataTable();
         using (var parser = new GenericParserAdapter(file) { ColumnDelimiter = Separator[0], FirstRowHasHeader = true })
            dt = parser.GetDataTable();
         using (var bcp = new SqlBulkCopy(GetDbConnection()))
         {
            bcp.BatchSize = 10000;
            bcp.DestinationTableName = "Misure.RawImportData";
            bcp.NotifyAfter = 10000;
            bcp.SqlRowsCopied += ShowBulkCopyActivity;
            bcp.BulkCopyTimeout = 600;
            bcp.WriteToServer(dt);
         }

         RowsWritten += dt.Rows.Count;
         LogTo.Trace($"Riversato il file {file} nel database. Scritte sinora {RowsWritten} righe.");
      }

      static void ShowBulkCopyActivity(object sender, SqlRowsCopiedEventArgs e) {
         if (Quiet) return;
         UpdateConsole("Preimportazione", RowsWritten + e.RowsCopied);
      }
   }
}