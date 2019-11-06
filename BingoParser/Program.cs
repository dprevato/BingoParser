using Anotar.NLog;
using NLog;
using NLog.Targets;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using static BingoParser.BulkImport;
using static BingoParser.ParseArguments;

namespace BingoParser
{
   public class Program
   {
      internal const string NULL = "-999";
      internal static long RowsWritten = 0;
      /// <summary>
      /// RowExpectedFormat
      /// Rappresenta una riga di testo composta dai seguenti elementi:
      /// - Una serie di caratteri alfanumerici (lettere e/o numeri e/o underscore)
      /// - Zero o più caratteri di separazione, seguiti da un carattere , o ; opzionale, seguiti da uno o più caratteri di separazione
      /// - Una data nel formato AAAAMMDDHHMMSS, compresa tra il 1900/01/01 00:00:00 e il 2099/12/31 23:59:59, con i secondi opzionali e 
      /// </summary>
      internal static string RowExpectedFormat { get; } = @"^\w+\s*[,;]?\s*(19|20)\d\d[- /.]?(0[1-9]|1[012])[- /.]?(0[1-9]|[12][0-9]|3[01])[T ]?([01][1-9]|2[0123]):?([0-5][0-9]):?([0-5][0-9])?\s*[,;]?\s*[-+]?[0-9]*[,\.]?[0-9]+\s*[,;]?\s*[-+]?[0-9]*[,\.]?[0-9]+.*?$";
      internal static CultureInfo LocCulture { get; } = CultureInfo.CreateSpecificCulture("en-US");
      internal static string[] HeaderData { get; } = { 
                                                      "SourceTable",
                                                      "SourceFilter",
                                                      "GaugeDateTime",
                                                      "GaugeText",
                                                      "GaugeValue",
                                                      "GaugeTaken"
                                                   };

      internal static StreamWriter OutputStream { get; set; }
      internal static long TotalRowsWritten { get; set; } = 0;

      public static void Main(string[] args) {
         LocCulture.DateTimeFormat.TimeSeparator = ":";
         LocCulture.DateTimeFormat.DateSeparator = "-";

         GetInfo();
         Parse(args);

#if DEBUG
#else
         var logConfig = new NLog.Config.LoggingConfiguration();
         var logFile = new FileTarget("logfile") {
                                                    FileName = @"${currentdir}/Logs/Log_${shortdate}.log",
                                                    Layout = @"${longdate} ${uppercase:${level}} ${message}"
                                                 };
         logConfig.AddTarget(logFile);
         logConfig.AddRuleForAllLevels(logFile);

         LogManager.Configuration = logConfig;
#endif

         if (!Directory.Exists(InputDirectory)) {
            Console.WriteLine($"La directory {InputDirectory} non esiste.");
            return;
         }

         if (!Directory.Exists(OutputDirectory)) Directory.CreateDirectory(OutputDirectory);

         //ConvertAllInputFiles();
         WriteAllTsvToServer();
#if DEBUG
         Console.ReadLine();
#endif
      }

      private static void GetInfo() {
         Console.WriteLine(@"BingoParse: Conversione file TSV e pre-importazione in Giusto!");
         Console.WriteLine(new string('-', 80));
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
         Console.WriteLine(new string('-', 80));
         Console.WriteLine(@"Daniele Prevato 2016-2019");
         Console.WriteLine(@"versione 1.0: (2016) Versione iniziale.");
         Console.WriteLine(@"versione 1.2: (2016) Aggiunta la creazione di un file di testo normalizzato per le misure provenienti");
         Console.WriteLine(@"                     dai vari impianti.");
         Console.WriteLine(@"versione 2.0: (2017) Aggiunta la funzionalità di preimportazione");
         Console.WriteLine(@"versione 3.0: (2019) Implementato il logging degli eventi di applicazione;");
         Console.WriteLine(@"                     Generalizzato l'input: qualsiasi file di testo (default: *.txt;*.csv;*.dbf) viene letto");
         Console.WriteLine(@"                     alla ricerca di misure valide.");
         Console.WriteLine(@"versione 3.1: (2019) Aggiunta la data di preimportazione;");
         Console.WriteLine(@"                     Il file tab-delimited di passaggio dei dati viene conservato al termine della procedura.");
         Console.WriteLine();
      }

      // Fase 1:  in questo ciclo vengono letti tutti i file di input e il loro contenuto viene scritto nel file di destinazione, che poi verrà
      //          importato nella tabella ImportSource
      [LogToErrorOnException]
      internal static void ConvertAllInputFiles() {
         var inputFiles = GetInputFileList(InputDirectory);
         LogTo.Trace($"Inizio conversione.");
         if (!inputFiles.Any()) {
            Console.WriteLine($"Nella directory {InputDirectory} non esistono file del tipo richiesto.");
            LogTo.Error($"Nessun file del tipo richiesto nella directory {InputDirectory}");
            return;
         }
         LogTo.Trace($"Trovati {inputFiles.Count} files nella directory {InputDirectory}");
         OutputStream = SetOutputStreamWriter($"{OutputDirectory}\\{OutputFile}");

         foreach (var file in inputFiles) {
            if (new FileInfo(file).Length == 0) {
               continue; // scarto i file vuoti
            }

            if (file.EndsWith(".DBF", StringComparison.InvariantCultureIgnoreCase))
               ConvertSingleDbfFile(file);
            else
               ConvertSingleTextFile(file);
         }

         OutputStream.Flush();
         OutputStream.Close();
         if (!Quiet) Console.WriteLine($"Sono state convertite in totale {TotalRowsWritten} righe.");

         LogTo.Info($"Conversione terminata - convertite {TotalRowsWritten} righe.\n");
      }

      private static void DeleteAllTsvFiles() {
         var files = Directory.GetFiles(OutputDirectory, "*.tsv");
         foreach (var f in files) {
            if(f != OutputFile) File.Delete(f);
         }
         LogTo.Trace($"File temporanei eliminati.\n");
      }

      internal static List<string> GetInputFileList(string inputDir) {
         // TODO: Questa funzione va generalizzata in modo che si possano aggiungere nuovi file di input senza dover ricompilare
         var inputFiles = new List<string>();

         foreach (var p in FilePatterns) {
            inputFiles.AddRange(Directory.GetFiles(inputDir, p));
         }

         LogTo.Info($"Trovati {inputFiles.Count} files nella directory di input.");
         return inputFiles;
      }

      [LogToErrorOnException]
      private static StreamWriter SetOutputStreamWriter(string dest) {
         if (File.Exists(dest)) File.Delete(dest);
         var destWriter = new StreamWriter(dest); // costante per tutto il ciclo
         destWriter.WriteLine(string.Join(Separator, HeaderData));
         return destWriter;
      }

      [LogToErrorOnException]
      private static void ConvertSingleDbfFile(string file) {
         // Qui viene convertito un singolo file DBF e il risultato viene scritto nel file di destinazione
         var fileName = Path.GetFileNameWithoutExtension(file);
         RowsWritten = 0;
         if (!Quiet) UpdateConsole(fileName, RowsWritten);
         var today = DateTime.Today.ToString("yyyy-MM-dd");
         var br = new BinaryReader(File.OpenRead(file));
         var buffer = br.ReadBytes(Marshal.SizeOf(typeof(DbfHeader))); // contiene i 32 byte dell'header
         var handle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
         var header = (DbfHeader) Marshal.PtrToStructure(handle.AddrOfPinnedObject(), typeof(DbfHeader));
         handle.Free();

         //  Ora header contiene l'header del file DBF, in forma tale da poter accedere ai singoli campi che la compongono.
         //  Leggo adesso la lista delle colonne, che sono una serie di blocchi da 32 byte, che continua finché il prossimo
         //  byte nel file non vale 0x0D (13).
         var columns = new List<DbfColumnDescriptor>();
         while (br.PeekChar() != 0x0d) {
            buffer = br.ReadBytes(Marshal.SizeOf(typeof(DbfColumnDescriptor)));
            handle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
            columns.Add((DbfColumnDescriptor) Marshal.PtrToStructure(handle.AddrOfPinnedObject(), typeof(DbfColumnDescriptor)));
            handle.Free();
         }

         //  Cerco la prima riga contenente dati
         ((FileStream) br.BaseStream).Seek(header.FirstDataRowOffset, SeekOrigin.Begin);

         //  E da qui in poi leggo le righe, fino alla fine del file
         for (var x = 1; x < header.NumberOfDataRows; x++) {
            buffer = br.ReadBytes(header.RecordLen);
            var singleRowReader = new BinaryReader(new MemoryStream(buffer));
            if (singleRowReader.ReadChar() == '*')
               continue; // Se il primo carattere della riga è '*', significa che la riga è cancellata

            var dataRow = columns.Aggregate("", (current, col) => current + $"{Encoding.ASCII.GetString(singleRowReader.ReadBytes(col.ColumnLength)).Trim()}{Separator}");
            dataRow.Remove(dataRow.Length - Separator.Length); // Toglie l'ultimo separatore
            dataRow = GetCleanDataRow(dataRow);

            var fieldData = dataRow.Split(new[] { Separator }, StringSplitOptions.None).ToList();

            // Devo ricavare la data dall'ultima colonna; se questa è vuota, ricavo la data dalle prime due colonne. La data vale per tutti i valori
            // contenuti nelle colonne
            var gdt = FormatDateForSql(fieldData.Last()) ?? FormatDateForSql(fieldData[0], fieldData[1]);

            if (string.IsNullOrEmpty(gdt)) continue; // se per la seconda volta gdt è vuota, significa che il formato della riga è sbagliato

            for (var col = 2; col < fieldData.Count - 2; col++) {
               if (ExcludeNulls && fieldData[col].IsNullValue()) continue; // non esporto le righe nulle
               OutputStream.WriteLine(string.Concat(Path.GetFileNameWithoutExtension(file),
                                                   Separator,
                                                   columns[col].ColumnName,
                                                   Separator,
                                                   gdt,
                                                   Separator,
                                                   string.Empty, /* I file DBF non contengono la misura strumentale RawValue */
                                                   Separator,
                                                   fieldData[col],
                                                   Separator,
                                                   today) );
               if (!Quiet) {
                  RowsWritten++;
                  if (RowsWritten % 1000 == 0) UpdateConsole(fileName, RowsWritten);
               }
            }
         }

         // Il ciclo principale è terminato, chiudo gli stream e aggiorno il contatore
         if (!Quiet) {
            if (RowsWritten > 0) UpdateConsole(fileName, RowsWritten);
            Console.WriteLine();
            TotalRowsWritten += RowsWritten;
         }

         br.Close();
         br.Dispose();
         OutputStream.Flush();

         LogTo.Info($"Convertito il file {file} - righe scritte {RowsWritten} (righe totali {TotalRowsWritten}).");
      }

      internal static void UpdateConsole(string message, long rows) {
         Console.Write(rows == 0 ? message : $"\r{message}: {rows.ToString().PadLeft(12)}");
      }

      [LogToErrorOnException]
      static void ConvertSingleTextFile(string file) {
         // In una futura versione, il codice può essere generalizzato specificando o ricavando dal file stesso informazioni come
         // numero di campi, separatore, header, ecc.
         // Ora il separatore è TAB, e i tracciati degli unici due file che contengono dati TSV sono i seguenti:
         // Site - Device - DateTime - GaugeValue - GaugeTaken
         // che sono le prime quattro colonne del file di destinazione. La quinta colonna è comunque aggiunta dal programma.
         var fileName = Path.GetFileNameWithoutExtension(file);
         RowsWritten = 0;
         if (!Quiet) UpdateConsole(fileName, RowsWritten);

         var today = DateTime.Today.ToString("yyyy-MM-dd");

         using (var source = new StreamReader(file)) {
            while (!source.EndOfStream) {
               var line = source.ReadLine();
               if (string.IsNullOrEmpty(line) || !Regex.IsMatch(line, RowExpectedFormat)) continue;
               line = GetCleanDataRow(line); // Contiene 4 campi gia separati da tab: Channel, GaugeDateTime, RawValue, GaugeValue
               var fieldData = line.Split(new[] { Separator }, StringSplitOptions.None).ToList();
               OutputStream.WriteLine(string.Concat(
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
                                                   today
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

         OutputStream.Flush();

         LogTo.Info($"Convertito il file {file} - righe scritte {RowsWritten} (righe totali {TotalRowsWritten}).");
      }

      public static string FormatDateForSql(string date, string time) {
         if (string.IsNullOrEmpty(date) || string.IsNullOrEmpty(time)) {
            LogTo.Error($"Formattazione data per SQL fallita, almeno uno dei due parametri è nullo");
            return null;
         }

         if (date.IsIsoDateTime()) return FormatDateForSql(date);

         if (date.IsIsoDate() && time.IsIsoTime())
            return $"{DateTime.ParseExact(date, "dd/MM/yyyy", LocCulture):yyyy-MM-dd} {time.Substring(0, 5).Replace('.', ':')}";
         return null;
      }

      public static string FormatDateForSql(string datetime) {
         if (datetime != null && datetime.IsIsoDateTime()) { // minimo aaaaMMdd
            datetime = datetime.Strip("-/.T:");
            var datePart = datetime.Substring(0, 8);
            var timePart = datetime.Remove(0, 8).Insert(4, ":").Insert(2, ":");
            if (timePart.EndsWith(":")) timePart = timePart.Remove(timePart.Length - 1, 1);
            return $"{DateTime.ParseExact(datePart, "yyyyMMdd", CultureInfo.InvariantCulture):yyyy-MM-dd} {timePart}";
         }

         return null;
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