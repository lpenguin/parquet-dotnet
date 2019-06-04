using System;
using System.Collections;
using System.IO;
using Parquet.Data;

namespace Parquet.Data.Concrete
{
   class DoubleDataTypeHandler : BasicPrimitiveDataTypeHandler<double>
   {
      public DoubleDataTypeHandler() : base(DataType.Double, Thrift.Type.DOUBLE)
      {

      }

      protected override double ReadSingle(BinaryReader reader, Thrift.SchemaElement tse, int length)
      {
         return reader.ReadDouble();
      }

      static byte[] GetBytes(double[] values) {
         var result = new byte[values.Length * sizeof(double)];
         Buffer.BlockCopy(values, 0, result, 0, result.Length);
         return result;
      }
      
      public override void Write(Thrift.SchemaElement tse, BinaryWriter writer, IList values, Thrift.Statistics statistics)
      {
         // casing to an array of TSystemType means we avoid Array.GetValue calls, which are slow
         var typedArray = (double[]) values;
         if (BitConverter.IsLittleEndian)
         {
            var bytes = GetBytes(typedArray);
            writer.BaseStream.Write(bytes, 0, bytes.Length);
         }
         else
         {
            foreach (var value in typedArray)
            {
               writer.Write(value);
            }
         }
      }
   }
}
