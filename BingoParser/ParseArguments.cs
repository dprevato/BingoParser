using System;
using System.IO;
using static BingoParser.Program;

namespace BingoParser
{
  /// <summary>
  /// Questa classe ha la funzione di interpretare la riga di comando e, se necessario, fornire valori di default. I parametri accettati sono i seguenti:
  /// -in=    rappresenta la directory di input, quella in cui si trovano i file da convertire. Per default, è la directory corrente.
  /// -out=   rappresenta la directory di output, quella in cui vanno messi i file convertiti. Per default, è la subdir CONVERTED sotto la directory di input
  /// -f=     è il nome del file, o il pattern che lo descrive, con o senza l'estensione .DBF, che viene aggiunta se non presente.
  /// -s=     è il carattere di separazione tra le colonne nei file convertiti. Per default, è il carattere di tabulazione \t.
  /// </summary>
  internal static class ParseArguments
  {
    public static string InputDirectory { get; private set; }
    public static string OutputDirectory { get; private set; }
    public static string FilePattern { get; private set; }
    public static string Separator { get; private set; }
    public static bool Quiet { get; private set; }
    public static string NormalOut { get; private set; }
    public static bool ExcludeNulls { get; private set; }

    /// <summary>
    /// Riceve il vettore di opzioni della riga di comando, lo interpreta e assegna i valori ai membri
    /// </summary>
    /// <param name="args"></param>
    internal static void Parse(string[] args) {
      // Così posso semplificare i test successivi
      for (var x = 0; x < args.Length; x++) args[x] = args[x].Strip().ToLowerInvariant();
      // Directory di input
      InputDirectory = @".\"; //  Questo è il valore di default, che rimane se nel ciclo non succede niente
      foreach (var v in args) {
        if (!v.StartsWith(@"-in=") && !v.StartsWith(@"/in=")) continue;
        if (Directory.Exists(v.Substring(4))) InputDirectory = v.Substring(4);
        if (!InputDirectory.EndsWith(@"\")) InputDirectory += @"\";
      }

      OutputDirectory = $"{InputDirectory}CONVERTED"; // i file vengono distinti dall'estensione, se non viene specificata una directory
      foreach (var v in args) {
        if (!v.StartsWith(@"-out=") && !v.StartsWith(@"/out=")) continue;
        OutputDirectory = v.Substring(5);
        if (Directory.Exists(OutputDirectory)) Directory.Delete(OutputDirectory, true);
        if (!OutputDirectory.EndsWith(@"\")) OutputDirectory += @"\";
      }

      NormalOut = @"AllGauges.txt";
      foreach (var v in args) {
        if (!v.StartsWith(@"-n=") && !v.StartsWith(@"/n=")) continue;
        NormalOut = v.Substring(3);
        if (!NormalOut.EndsWith(@".txt", StringComparison.InvariantCultureIgnoreCase)) NormalOut += ".txt";
      }

      FilePattern = @"*.DBF";
      foreach (var v in args) {
        if (!v.StartsWith(@"-f=") && !v.StartsWith(@"/f=")) continue;
        FilePattern = v.Strip().Substring(3);
        if (!FilePattern.EndsWith(".DBF", StringComparison.InvariantCultureIgnoreCase)) FilePattern += @".DBF";
      }

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
    }
  }

  internal static class MyExtensions
  {
    internal static string Strip(this string s) { return s.Replace(" ", ""); }

    internal static bool IsNullValue(this string s) {
      return string.IsNullOrWhiteSpace(s) || s.Trim() == NULL;
    }
  }

}