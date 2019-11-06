using Anotar.NLog;
using LumenWorks.Framework.IO.Csv;
using System;
using System.Data.SqlClient;
using System.IO;
using static BingoParser.ParseArguments;
using static BingoParser.Program;

namespace BingoParser
{
   public static class BulkImport
   {
      // Qui si esegue la preimportazione in Giusto!2010
      public static string GetDbConnection() {
         try {
#if DEBUG
            return "Server=localhost;Database=G2009-Test;Connection Timeout=400;MultipleActiveResultSets=True;App=EntityFramework;User Id=sa;Password=5T0c477o";
#else
            return "Server=digheidro.master.local;Database=G2009-test; User ID=rwuser; Password=iren2016; App=EntityFramework;";
#endif
         }
         catch (Exception e)
         {
            LogTo.Error($"{e.Source}\n{e.Message}");
            return e.Message;
         }

      }


      public static void WriteAllTsvToServer() {
         try {
            var DecimalSeparator = LocCulture.NumberFormat.NumberDecimalSeparator;

            RowsWritten = 0;
            LogTo.Info($"Inizio Preimportazione.");
            WriteSingleTsvToServer($"{OutputDirectory}{OutputFile}");
            UpdateConsole("\nNumero totale di righe trasferite", TotalRowsWritten);
            LogTo.Info($"Preimportazione terminata.");
         }
         catch(SqlException sqlex) {
            LogTo.Error($"Errore in preimportazione: Problema nel collegamento al database.\n{sqlex.Source}\n{sqlex.Message}");
            Console.WriteLine(@"Errore in preimportazione: problema di collegamento al database.");
         }
         catch(Exception e) {
            LogTo.Error($"Errore in preimportazione: possibile mancanza del file di input.\n{e.Source}\n{e.Message}");
            Console.WriteLine(@"Errore in preimportazione: possibile mancanza del file di input.");
         }
      }


      [LogToErrorOnException]
      private static void WriteSingleTsvToServer(string file) {
         using (var sr = new StreamReader(file))
         using (var dt = new CsvReader(sr, true, Separator[0]))
         using (var bcp = new SqlBulkCopy(GetDbConnection()))
         {

            bcp.DestinationTableName = "Misure.RawImportData";
            bcp.ColumnMappings.Add("SourceTable", "SourceTable");
            bcp.ColumnMappings.Add("SourceFilter", "SourceFilter");
            bcp.ColumnMappings.Add("GaugeDateTime", "GaugeDateTime");
            bcp.ColumnMappings.Add("GaugeText", "GaugeText");
            bcp.ColumnMappings.Add("GaugeValue", "GaugeValue");
            bcp.ColumnMappings.Add("GaugeTaken", "GaugeTaken");

            bcp.NotifyAfter = 10000;
            bcp.SqlRowsCopied += ShowBulkCopyActivity;
            bcp.BulkCopyTimeout = 3600;
            bcp.WriteToServer(dt);
         }
      }

      private static void ShowBulkCopyActivity(object sender, SqlRowsCopiedEventArgs e) {
         if (Quiet)
            return;
         UpdateConsole("Preimportazione", RowsWritten + e.RowsCopied);
      }
   }
}