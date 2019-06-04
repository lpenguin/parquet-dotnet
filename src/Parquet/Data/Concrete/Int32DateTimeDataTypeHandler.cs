using System;
using System.Collections;
using System.IO;
using Parquet.Data;
using Parquet.Thrift;

namespace Parquet.Data.Concrete
{
   class Int32DateTimeDataTypeHandler : BasicPrimitiveDataTypeHandler<Int32>
   {
      public Int32DateTimeDataTypeHandler() : base(DataType.Int32Date, Thrift.Type.INT32, ConvertedType.DATE)
      {
      }

      public override bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions)
      {
         return tse.__isset.type && tse.Type == Thrift.Type.INT32
                                 && tse.__isset.converted_type
                                 && tse.Converted_type == ConvertedType.DATE;
      }
      
      protected override Int32 ReadSingle(BinaryReader reader, Thrift.SchemaElement tse, int length)
      {
         return reader.ReadInt32();
      }

      protected override void WriteOne(BinaryWriter writer, Int32 value)
      {
         writer.Write(value);
      }
   }
}