using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Streamer.CLI
{
    /// <summary>
    /// Wrapper for a nice console table.
    /// </summary>
    public class ConsoleTable
    {
        private int _columnCount;

        private string[] _columns;
        private readonly int _widthOfColumn;
        private int _rowCount;

        private string[,] _values { get; }

        public ConsoleTable(string[] columns, int expectedRows, int widthOfColumn = 10)
        {
            this._columnCount = columns.Length;

            _columns = new string[_columnCount];

            // copy (and trim if needed) columns
            for (int i = 0; i < _columnCount; i++)
            {
                var name = columns[i];
                if (name.Length > widthOfColumn)
                {
                    name = name.Substring(0, widthOfColumn - 3) + "...";
                }
                _columns[i] = name;
            }

            //this._columns = columns;
            this._widthOfColumn = widthOfColumn;
            this._values = new string[_rowCount = expectedRows, _columnCount];
        }

        public void Render()
        {
            Console.Clear();

            Console.WriteLine(GenerateHeaderRow());

            for (int i = 0; i < _rowCount; i++)
            {
                Console.WriteLine(GenerateEmptyRow());
            }
        }

        private string GenerateHeaderRow()
        {
            var builder = new StringBuilder();

            var header = string.Join('|', _columns.Select(x => x.PadLeft(_widthOfColumn)));
            builder.AppendLine(header);
            builder.Append("".PadRight(header.Length, '-'));

            return builder.ToString();
        }

        private string GenerateEmptyRow()
        {
            var builder = new StringBuilder();

            builder.Append(string.Join('|', _columns.Select(x => "".PadLeft(_widthOfColumn))));
            //builder.Append("\r\n".PadLeft(builder.Length, '-'));

            return builder.ToString();
        }

        public void SetValue(int column, int row, string value)
        {
            System.Diagnostics.Debug.WriteLine($"{column} x {row} x {value}");
            if (value.Length > _widthOfColumn)
            {
                value = value.Substring(0, _widthOfColumn - 3) + "...";
            }

            value = value.PadLeft(_widthOfColumn);

            lock (Console.Out)
            {
                // calculate the position
                Console.SetCursorPosition(column * (_widthOfColumn + 1), row + 2);
                Console.Write(value);
            }
        }
    }
}
