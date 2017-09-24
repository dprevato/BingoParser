using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using static BingoParser.ParseArguments;

namespace BingoParser
{
  public class Program
  {
    public static long RowsWritten = 0;
    public static string NULL { get; } = "-999";
    public static CultureInfo LocCulture { get; } = CultureInfo.CreateSpecificCulture("en-US");

    static void Main(string[] args) {
#if DEBUG
      const string DbConnectionString =
        @"Provider=SQLNCLI11;Server=Gandalf\SQLEXPRESS;Database=G2009-Test;Trusted_Connection=yes;";
#else
      const string DbConnectionString =
        @"Provider=SQLNCLI11;Server=digheidro.master.local;Database=G2009-Test;User=RWUser;Password=iren2016;";
#endif
      LocCulture.DateTimeFormat.TimeSeparator = ":";
      LocCulture.DateTimeFormat.DateSeparator = "-";

      var DbConnection = new OleDbConnection(DbConnectionString);

      GetInfo();
      Parse(args);

      if (!Directory.Exists(InputDirectory)) {
        Console.WriteLine($"La directory {InputDirectory} non esiste.");
        return;
      }

      if (!Directory.Exists(OutputDirectory)) Directory.CreateDirectory(OutputDirectory);

      ConvertAllInputFiles();
    }

    static void GetInfo() {
      Console.WriteLine(@"BingoParse: Conversione file TSV e pre-importazione in Giusto!");
      Console.WriteLine(new String('-', 80));
      Console.WriteLine(@"Un'utility per la conversione di file dBase III/FoxPro in testo delimitato e per");
      Console.WriteLine(@"la preimportazione dei dati convertiti in SQL Server");
      Console.WriteLine(@"Opzioni di avvio:");
      Console.WriteLine(@"-in=<DirIn> directory di input - default: la directory corrente");
      Console.WriteLine(@"-out=<DirOut> directory di output - default: DirIn\CONVERTED");
      Console.WriteLine(@"-f=<file> nome del file o dei file da convertire - default: *.DBF");
      Console.WriteLine(@"-s=<separator> separatore di campo - default: tab \t");
      Console.WriteLine(@"-q be quiet - messaggi di avanzamento soppressi");
      Console.WriteLine(@"-all esporta tutte le letture, anche quelle nulle (default: NO)");
      Console.WriteLine(@"-n=<NomeFileNormalizzato> nome del file contenente l'output normalizzato");
      Console.WriteLine(new String('-', 80));
      Console.WriteLine(@"BingoSoft 2016-2017");
      Console.WriteLine(@"versione 1.0:");
      Console.WriteLine(@"- Versione iniziale.");
      Console.WriteLine(@"versione 1.2:");
      Console.WriteLine(
        @"- Aggiunta la creazione di un file di testo normalizzato per le misure provenienti da file DBF.");
      Console.WriteLine(@"versione 2.0");
      Console.WriteLine(@"- Aggiunta la funzionalità di preimportazione.");
    }

    // Fase 1:  in questo ciclo vengono letti tutti i file di input e il loro contenuto viene scritto nel file di destinazione, che poi verrà
    //          importato nella tabella ImportSource
    static void ConvertAllInputFiles() {
      var inputFiles = GetInputFileList();
      if (!inputFiles.Any()) {
        Console.WriteLine($"Nella directory {InputDirectory} non esistono file del tipo richiesto.");
        return;
      }

      var destination = SetupDestinationStreamWriter($"{OutputDirectory}\\{NormalOut}");

      foreach (var file in inputFiles) {
        if (new FileInfo(file).Length == 0) continue; // scarto i file vuoti
        if (file.EndsWith(".DBF", StringComparison.InvariantCultureIgnoreCase)) ConvertSingleDBFFile(file, destination);
        else if (file.EndsWith(".TXT", StringComparison.InvariantCultureIgnoreCase))
          ConvertSingleTextFile(file, destination);
      }
      destination.Close();
      destination.Dispose();
    }

    static List<string> GetInputFileList() {
      // TODO: Questa funzione va generalizzata in modo che si possano aggiungere nuovi file di input senza dover ricompilare
      var inputFiles = new List<string>();

      inputFiles.AddRange(Directory.GetFiles(InputDirectory, "*.dbf"));
      inputFiles.Add($"{InputDirectory}clareamisure.txt");
      inputFiles.Add($"{InputDirectory}gorgemisure.txt");
      return inputFiles;
    }

    static StreamWriter SetupDestinationStreamWriter(string dest) {
      if (File.Exists(dest)) File.Delete(dest);
      var destWriter = new StreamWriter(dest); // costante per tutto il ciclo
      destWriter.WriteLine(
        $"Site{Separator}PdmId{Separator}GaugeDateTime{Separator}RawValue{Separator}GaugeValue{Separator}GaugeTaken");
      return destWriter;
    }

    static void ConvertSingleDBFFile(string file, TextWriter destination) {
      // Qui viene convertito un singolo file DBF e il risultato viene scritto nel file di destinazione
      if (Quiet) Console.Write(".");
      else Console.WriteLine(Path.GetFileNameWithoutExtension(file));

      var br = new BinaryReader(File.OpenRead(file));
      var buffer = br.ReadBytes(Marshal.SizeOf(typeof(DbfHeader))); // contiene i 32 byte dell'header
      var handle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
      var header = (DbfHeader)Marshal.PtrToStructure(handle.AddrOfPinnedObject(), typeof(DbfHeader));
      handle.Free();
      //  Ora header contiene l'header del file DBF, in forma tale da poter accedere ai singoli campi che la compongono.
      //  Leggo adesso la lista delle colonne, che sono una serie di blocchi da 32 byte, che continua finché il prossimo
      //  byte nel file non vale 0x0D (13).
      var columns = new List<DbfColumnDescriptor>();
      while (br.PeekChar() != 0x0d) {
        buffer = br.ReadBytes(Marshal.SizeOf(typeof(DbfColumnDescriptor)));
        handle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
        columns.Add(
          (DbfColumnDescriptor)Marshal.PtrToStructure(handle.AddrOfPinnedObject(), typeof(DbfColumnDescriptor)));
        handle.Free();
      }

      //  Cerco la prima riga contenente dati
      ((FileStream)br.BaseStream).Seek(header.FirstDataRowOffset, SeekOrigin.Begin);

      //  E da qui in poi leggo le righe, fino alla fine del file
      for (var x = 1; x < header.NumberOfDataRows; x++) {
        buffer = br.ReadBytes(header.RecordLen);
        var SingleRowReader = new BinaryReader(new MemoryStream(buffer));
        if (SingleRowReader.ReadChar() == '*')
          continue; // Se il primo carattere della riga è '*', significa che la riga è cancellata

        var dataRow = columns.Aggregate("",
          (current, col) =>
            current + $"{Encoding.ASCII.GetString(SingleRowReader.ReadBytes(col.ColumnLength)).Trim()}{Separator}");
        dataRow.Remove(dataRow.Length - Separator.Length); // Toglie l'ultimo separatore
        dataRow = GetCleanDataRow(dataRow);

        var fieldData = dataRow.Split(new[] { Separator }, StringSplitOptions.None).ToList();

        // Devo ricavare la data dall'ultima colonna; se questa è vuota, ricavo la data dalle prime due colonne. La data vale per tutti i valori
        // contenuti nelle colonne
        var gdt = fieldData.Last();
        if (gdt.Length == 0) {
          gdt = FormatDateForSql(fieldData[0], fieldData[1]);
        }
        if (gdt.Length == 0)
          continue; // se per la seconda volta gdt è vuota, significa che il formato della riga è sbagliato

        for (var col = 2; col < fieldData.Count - 2; col++) {
          if (ExcludeNulls && fieldData[col].IsNullValue()) continue; // non esporto le righe nulle
          destination.WriteLine(String.Concat(Path.GetFileNameWithoutExtension(file),
            Separator,
            columns[col].ColumnName,
            Separator,
            gdt,
            Separator,
            String.Empty, /* I file DBF non contengono la misura strumentale RawValue */
            Separator,
            fieldData[col],
            Separator,
            DateTime.Today.ToString("yyyy-MM-dd")));
          RowsWritten++;
        }
      }
      // Il ciclo principale è terminato, chiudo gli stream
      br.Close();
      br.Dispose();
      destination.Flush(); // destination resta aperto
    }

    static void ConvertSingleTextFile(String file, StreamWriter destination) {
      // In una futura versione, il codice può essere generalizzato specificando o ricavando dal file stesso informazioni come
      // numero di campi, separatore, header, ecc.
      // Ora il separatore è TAB, e i tracciati degli unici due file che contengono dati TSV sono i seguenti:
      // Site - Device - DateTime - GaugeValue
      // che sono le prime quattro colonne del file di destinazione. La quinta colonna è comunque aggiunta dal programma.
      if (Quiet) Console.Write(".");
      else Console.WriteLine(Path.GetFileNameWithoutExtension(file));

      using (var source = new StreamReader(file)) {
        while (!source.EndOfStream) {
          var line = source.ReadLine();
          if (line.Length == 0) continue;
          line = GetCleanDataRow(line); // Contiene i 4 campi gia separati da tab
          var fieldData = line.Split(new[] { Separator }, StringSplitOptions.None).ToList();


          line = $"{line}{Separator}{DateTime.Today:yyyy-MM-dd}";
          destination.WriteLine(line);
        }
      }
      destination.Flush();
    }

    public static string FormatDateForSql(string date, string time) {
      try {
        return
          $"{DateTime.ParseExact(date, "dd/MM/yyyy", LocCulture):yyyy-MM-dd} {time.Substring(0, 5)}";
      }
      catch (FormatException) {
        return string.Empty;
      }
    }

    public static string FormatDateForSql(string datetime) {
      var datePart = datetime.Substring(0, 8);
      var timePart = datetime.Remove(0, 8).Insert(2, ":");

      try {
        return
          $"{DateTime.ParseExact(datePart, "yyyyMMdd", LocCulture):yyyy-MM-dd} {timePart}";
      }
      catch (FormatException) {
        return string.Empty;
      }
    }

    public static string GetCleanDataRow(string dataRow) {
      dataRow = dataRow.Replace("-99999", "-999")
                       .Replace("****", "-999")
                       .Replace(",", ".")
                       .Replace(" ", "")
                       .Replace("-999.00", "-999");

      if (dataRow.EndsWith("\t")) dataRow = dataRow.Remove(dataRow.Length - 1);
      return dataRow;
    }
  }
}