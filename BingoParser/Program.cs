using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using static BingoParser.BulkImport;
using static BingoParser.ParseArguments;

namespace BingoParser
{
  public class Program
  {
    public static long RowsWritten = 0;
    public static string NULL { get; } = "-999";
    public static CultureInfo LocCulture { get; } = CultureInfo.CreateSpecificCulture("en-US");
    public static List<string> TsvFileNames { get; set; }
    public static string[] HeaderData { get; } = {
      "SourceTable",
      "SourceFilter",
      "GaugeDateTime",
      "GaugeText",
      "GaugeValue",
      "GaugeTaken"
    };

    public static long TotalRowsWritten { get; set; } = 0;

    public static void Main(string[] args) {
      LocCulture.DateTimeFormat.TimeSeparator = ":";
      LocCulture.DateTimeFormat.DateSeparator = "-";

      GetInfo();
      Parse(args);

      if (!Directory.Exists(InputDirectory)) {
        Console.WriteLine($"La directory {InputDirectory} non esiste.");
        return;
      }

      if (!Directory.Exists(OutputDirectory)) Directory.CreateDirectory(OutputDirectory);

      ConvertAllInputFiles();

      WriteAllTsvToServer();
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
      Console.WriteLine(@"- Aggiunta la creazione di un file di testo normalizzato per le misure provenienti da file DBF.");
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

      foreach (var file in inputFiles) {
        if (new FileInfo(file).Length == 0) continue; // scarto i file vuoti
        if (file.EndsWith(".DBF", StringComparison.InvariantCultureIgnoreCase)) ConvertSingleDBFFile(file);
        else if (file.EndsWith(".TXT", StringComparison.InvariantCultureIgnoreCase) || file.EndsWith(".CSV", StringComparison.InvariantCultureIgnoreCase))
          ConvertSingleTextFile(file);
      }
      if (!Quiet) Console.WriteLine($"Sono state convertite in totale {TotalRowsWritten} righe.");
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
      destWriter.WriteLine(String.Join(Separator, HeaderData));
      return destWriter;
    }

    static void ConvertSingleDBFFile(string file) {
      // Qui viene convertito un singolo file DBF e il risultato viene scritto nel file di destinazione
      var fileName = Path.GetFileNameWithoutExtension(file);
      RowsWritten = 0;
      if (!Quiet) UpdateConsole(fileName, RowsWritten);
      var destination = SetupDestinationStreamWriter($"{OutputDirectory}\\{fileName}.tsv");

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
        var gdt = FormatDateForSql(fieldData.Last());
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
          if (!Quiet) {
            RowsWritten++;
            if (RowsWritten % 1000 == 0) UpdateConsole(fileName, RowsWritten);
          }
        }
      }
      // Il ciclo principale è terminato, chiudo gli stream e aggiorno il contatore
      if (!Quiet) {
        UpdateConsole(fileName, RowsWritten);
        Console.WriteLine();
        TotalRowsWritten += RowsWritten;
      }
      br.Close();
      br.Dispose();
      destination.Flush();
      destination.Close();
    }

    public static void UpdateConsole(string destination, long rows) {
      if (rows == 0) {
        Console.Write(destination);
      }
      else {
        Console.Write($"\r{destination}: righe scritte {rows.ToString().PadLeft(12)}");
      }
    }

    static void ConvertSingleTextFile(String file) {
      // In una futura versione, il codice può essere generalizzato specificando o ricavando dal file stesso informazioni come
      // numero di campi, separatore, header, ecc.
      // Ora il separatore è TAB, e i tracciati degli unici due file che contengono dati TSV sono i seguenti:
      // Site - Device - DateTime - GaugeValue
      // che sono le prime quattro colonne del file di destinazione. La quinta colonna è comunque aggiunta dal programma.
      var fileName = Path.GetFileNameWithoutExtension(file);
      RowsWritten = 0;
      if (!Quiet) UpdateConsole(fileName, RowsWritten);

      var destination = SetupDestinationStreamWriter($"{OutputDirectory}\\converted_{fileName}.tsv");

      using (var source = new StreamReader(file)) {
        while (!source.EndOfStream) {
          var line = source.ReadLine();
          if (line.Length == 0) continue;
          line = GetCleanDataRow(line); // Contiene 4 campi gia separati da tab: Channel, GaugeDateTime, RawValue, GaugeValue
          var fieldData = line.Split(new[] { Separator }, StringSplitOptions.None).ToList();
          destination.WriteLine(String.Concat(
            Path.GetFileNameWithoutExtension(file),
            Separator,
            fieldData[0],
            Separator,
            FormatDateForSql(fieldData[1]),
            Separator,
            fieldData[2],
            Separator,
            fieldData[3],
            Separator,
            DateTime.Today.ToString("yyyy-MM-dd")
          ));
          if (!Quiet) {
            RowsWritten++;
            if (RowsWritten % 1000 == 0) UpdateConsole(fileName, RowsWritten);
          }
        }
      }
      if (!Quiet) {
        UpdateConsole(fileName, RowsWritten);
        Console.WriteLine();
        TotalRowsWritten += RowsWritten;
      }
      destination.Flush();
      destination.Close();
    }

    public static string FormatDateForSql(string date, string time) {
      try {
        if (date.Length == 12 && !date.Contains("/")) {
          return FormatDateForSql(date);
        }

        return $"{DateTime.ParseExact(date, "dd/MM/yyyy", LocCulture):yyyy-MM-dd} {time.Substring(0, 5).Replace('.', ':')}";
      }
      catch (FormatException) {
        return string.Empty;
      }
    }

    public static string FormatDateForSql(string datetime) {
      try {
        var datePart = datetime.Substring(0, 8);
        var timePart = datetime.Remove(0, 8).Insert(2, ":");
        return
          $"{DateTime.ParseExact(datePart, "yyyyMMdd", LocCulture):yyyy-MM-dd} {timePart}";
      }
      catch (Exception) {
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