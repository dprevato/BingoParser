using System;
using System.Runtime.InteropServices;

namespace BingoParser
{
  /// <summary>
  /// DbfHeader rappresenta la struttura di un header DBF classico (escluso dBase versione 7).
  /// L'header hauyna lunghezza fissa di 32 byte.
  /// </summary>
  [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Ansi, Pack = 1)]
  public struct DbfHeader
  {
    public byte Version; // per file dBase III+ o FoxBase vale 0x03
    public byte LastUpdateYear; // anni trascorsi dal 1900; valori possibili: 1900 - 2155
    public byte LastUpdateMonth;
    public byte LastUpdateDay;
    public int NumberOfDataRows; // Numero di righe di dati presenti nel file DBF; 
    public short FirstDataRowOffset; // posizione della prima riga di dati
    public short RecordLen; // Lunghezza di ogni riga di dati, compreso il flag di cancellazione '*'
    [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 20)] public string Filler;
  }

  /// <summary>
  /// Immediatamente dopo il blocco di header, il file DBF contiene un numero di blocchi da 32 bytes, ognuno dei quali
  /// descrive una singola colonna di dati. Il termine della serie di descrittori è marcato da un byte di valore 0x0D
  /// </summary>
  [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Ansi, Pack = 1)]
  public struct DbfColumnDescriptor
  {
    [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 11)]
    public string ColumnName; // 11 byte, riportano il nome della colonna, che non può essere più lungo di 10 caratteri
    public char ColumnType; // codice che rappresenta il tipo di dati della colonna
    public int ColumnPosition; // offset della colonna della colonna di dati (4 byte)
    public byte ColumnLength; // Lunghezza della colonna dati
    [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 15)] public string Filler;
  }


}