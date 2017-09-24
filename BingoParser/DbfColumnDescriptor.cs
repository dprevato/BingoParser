using System;
using System.Runtime.InteropServices;

namespace BingoParser
{
  /// <summary>
  /// Immediatamente dopo il blocco di header, il file DBF contiene un numero di blocchi da 32 bytes, ognuno dei quali
  /// descrive una singola colonna di dati. Il termine della serie di descrittori è marcato da un byte di valore 0x0D
  /// </summary>
  [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Ansi, Pack = 1)]
  public struct DbfColumnDescriptor
  {
    [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 11)]
    public string ColumnName;  // 11 byte, riportano il nome della colonna, che non può essere più lungo di 10 caratteri
    public char ColumnType; // codice che rappresenta il tipo di dati della colonna
    public Int32 ColumnPosition; // offset della colonna della colonna di dati (4 byte)
    public byte ColumnLength; // Lunghezza della colonna dati
    [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 15)] public string Filler;
  }
}
