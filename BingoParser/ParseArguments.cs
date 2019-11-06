using Anotar.NLog;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using static BingoParser.Program;

namespace BingoParser
{
   /// <summary>
   /// Questa classe ha la funzione di interpretare la riga di comando e, se necessario, fornire valori di default. I parametri accettati sono i seguenti:
   /// -in=    rappresenta la directory di input, quella in cui si trovano i file da convertire. Per default, è la directory corrente.
   /// -out=   rappresenta la directory di output, quella in cui vanno messi i file convertiti. Per default, è la subdir CONVERTED sotto la directory di input
   /// -f=     è il nome del file, o il pattern che lo descrive, con o senza l'estensione .DBF, che viene aggiunta se non presente.
   ///         20181129: il significato del parametro cambia: si tratta ora del pattern dei file da convertire. Sono ammessi pattern multipli, separati da
   ///                   punti e virgola. Il default è "*.dbf;*.txt;*.csv".
   ///         20190919: Le estensioni di default vengono comunque inserite nel pattern; eventuali estensioni specificate dal parametro /f vengono aggiunte
   ///                   a quelle di default.
   /// -s=     è il carattere di separazione tra le colonne nei file convertiti. Per default, è il carattere di tabulazione \t.
   /// </summary>
   internal static class ParseArguments
   {
      internal static string InputDirectory { get; private set; }
      internal static string OutputDirectory { get; private set; }
      internal static string FilePattern { get; private set; }
      internal static List<string> FilePatterns { get; set; }
      internal static string Separator { get; private set; }
      internal static bool Quiet { get; private set; }
      internal static string OutputFile { get; private set; }
      internal static bool ExcludeNulls { get; private set; }
      internal static string BulkImportTableName { get; private set; }
      internal static string ConnectionString { get; private set; }

      /// <summary>
      /// Riceve il vettore di opzioni della riga di comando, lo interpreta e assegna i valori ai membri
      /// </summary>
      /// <param name="args"></param>
      internal static void Parse(string[] args) {
         // Così posso semplificare i test successivi
         for (var x = 0; x < args.Length; x++) args[x] = args[x].Strip().ToLowerInvariant();

         // Directory di input
#if DEBUG
         InputDirectory = @"D:\Sandbox\DigheIdro\";
#else
         InputDirectory = @".\"; //  Questo è il valore di default, che rimane se nel ciclo non succede niente
#endif
         //foreach(var v in args) {
         //   if(Regex.IsMatch(v, @"^[-/]local$")) {
         //      // Se voglio eseguire in locale la procedura, devo specificare la stringa locale di connessione; altrimenti, inserisco quella di default IREN
         //      ConnectionString = "Server=localhost;Database=G2009-Test;Connection Timeout=400;MultipleActiveResultSets=True;App=EntityFramework;User Id=sa;Password=5T0c477o";
         //      InputDirectory = @"D:\Sandbox\DigheIdro\";
         //   }
         //   else {
         //      ConnectionString = "Server=digheidro.master.local;Database=G2009-test; User ID=rwuser; Password=iren2016; App=EntityFramework;";
         //      InputDirectory = @".\"; 
         //   }
         //}

         foreach (var v in args) {
            if (!Regex.IsMatch(v, @"^[-/][iI][nN]=.+$")) continue;
            if (Directory.Exists(v.Substring(4))) InputDirectory = v.Substring(4);
         }
         if (!InputDirectory.EndsWith(@"\")) InputDirectory += @"\";

         OutputDirectory = $@"{InputDirectory}"; // Il file di destinazione viene scritto nella stessa directory dove si trovano i file di input
         foreach (var v in args) {
            if (Regex.IsMatch(v, @"^[-/][Oo][Uu][Tt]=.+$")) OutputDirectory = v.Substring(5); // non è necessario fare il test su maiuscole e minuscole; è stato tutto trasfromato in minuscole
         }
         if (!OutputDirectory.EndsWith(@"\")) OutputDirectory += @"\";

         OutputFile = $@"AllReadings.tsv";
         foreach (var v in args) {
            if (!v.StartsWith(@"-n=") && !v.StartsWith(@"/n=")) continue;
            OutputFile = v.Substring(3);
            if (!OutputFile.EndsWith(@".tsv", StringComparison.InvariantCultureIgnoreCase)) OutputFile += ".tsv";
         }
   
         FilePattern = @"*.dbf;*.txt;*.csv";
         foreach (var v in args) {
            if (!v.StartsWith(@"-f=") && !v.StartsWith(@"/f=")) continue;
            FilePattern += $";{v.Strip().Substring(3)}";
         }
         FilePatterns = FilePattern.Strip().Split(';').ToList();
         LogTo.Info($"Estensioni selezionate: {FilePattern}");
         //if (!FilePatterns.Contains("*.dbf", StringComparer.InvariantCultureIgnoreCase)) FilePatterns.Add("*.dbf"); // .DBF ci deve essere

         Separator = "\t";
         foreach (var v in args) {
            if (!v.StartsWith("-s=") && !v.StartsWith("/s=")) continue;
            Separator = v.Substring(3);
         }

         Quiet = false;
         foreach (var v in args) {
            if (v == "-q" || v == "/q") Quiet = true;
         }

         ExcludeNulls = true;
         foreach (var v in args) {
            if (v == "-all" || v == "/all") ExcludeNulls = false;
         }

         BulkImportTableName = "Misure.RawImportData";
         foreach (var v in args) {
            if (Regex.IsMatch(v, @"^[-/][Tt]=.+$")) BulkImportTableName = v.Substring(3);
         }
      }
   }

   internal static class MyExtensions
   {
      /// <summary>
      /// Elimina gli spazi da una stringa di testo
      /// </summary>
      /// <param name="s"></param>
      /// <returns>La stringa priva degli spazi rimossi</returns>
      internal static string Strip(this string s) {
         return s.Replace(" ", string.Empty);
      }

      /// <summary>
      /// Elimina tutti i caratteri paassati nell'array c
      /// </summary>
      /// <param name="s">stringa da midificare</param>
      /// <param name="c">array di caratteri da eliminare</param>
      /// <returns>la stringa ripulita</returns>
      internal static string Strip(this string s, string c) {
         foreach (var t in c) {
            s = s.Replace(t, char.MinValue);
         }

         return s.Strip();
      }

      /// <summary>
      /// Verifica se una variabile string vale null, è vuota, o se contiene soltanto spazi.
      /// </summary>
      /// <param name="s"></param>
      /// <returns>true se la stringa non contiene caratteri significativi</returns>
      internal static bool IsNullValue(this string s) {
         return string.IsNullOrWhiteSpace(s) || s.Trim() == NULL;
      }

      internal static bool IsIsoDate(this string s) {
         return Regex.IsMatch(s, @"^(19|20)\d\d[-_ /.]?(0[1-9]|1[012])[-_ /.]?(0[1-9]|[12][0-9]|3[01])$");
      }

      internal static bool IsIsoDateTime(this string s) {
         return Regex.IsMatch(s, @"^(19|20)\d\d[-_ /.]?(0[1-9]|1[012])[-_ /.]?(0[1-9]|[12][0-9]|3[01])[ T]?([0-5][0-9])[:.]?([0-5][0-9])[:.]?([0-5][0-9])?$");
      }

      internal static bool IsIsoTime(this string t) {
         return Regex.IsMatch(t, @"^([0-5][0-9])[:.]?([0-5][0-9])([:.]?([0-5][0-9]))?$");
      }


   }
}