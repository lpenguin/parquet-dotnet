using System;
using System.Buffers;
using System.IO;
using Parquet.Thrift;

namespace Parquet.Data.Concrete
{
   class ByteArrayStringDataTypeHandler : BasicDataTypeHandler<byte[]>
   {
      private static readonly ArrayPool<byte> _bytePool = ArrayPool<byte>.Shared;
      private static readonly ArrayPool<byte[]> _byteArrayPool = ArrayPool<byte[]>.Shared;

      public ByteArrayStringDataTypeHandler() : base(DataType.ByteArrayString, Thrift.Type.BYTE_ARRAY, ConvertedType.UTF8)
      {
      }

      public override Array GetArray(int minCount, bool rent, bool isNullable)
      {
         if (rent)
         {
            return _byteArrayPool.Rent(minCount);
         }

         return new byte[minCount][];
      }

      public override bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions)
      {
         return tse.__isset.type && tse.Type == Thrift.Type.BYTE_ARRAY
                                 && tse.__isset.converted_type
                                 && tse.Converted_type == ConvertedType.UTF8;
      }

      public override int Read(BinaryReader reader, Thrift.SchemaElement tse, Array dest, int offset)
      {
         int remLength = (int)(reader.BaseStream.Length - reader.BaseStream.Position);

         if (remLength == 0)
            return 0;

         byte[][] tdest = (byte[][])dest;

         //reading byte[] one by one is extremely slow, read all data

         byte[] allBytes = _bytePool.Rent(remLength);
         reader.BaseStream.Read(allBytes, 0, remLength);
         int destIdx = offset;
         try
         {
            Span<byte> span = allBytes.AsSpan(0, remLength);   //will be passed as input in future versions

            int spanIdx = 0;

            while (spanIdx < span.Length && destIdx < tdest.Length)
            {
               int length = span.Slice(spanIdx, 4).ReadInt32();
               tdest[destIdx++] = span.Slice(spanIdx + 4, length).ToArray();
               spanIdx = spanIdx + 4 + length;
            }
         }
         finally
         {
            _bytePool.Return(allBytes);
         }

         return destIdx - offset;
      }

      protected override byte[] ReadSingle(BinaryReader reader, Thrift.SchemaElement tse, int length)
      {
         //length
         if(length == -1) length = reader.ReadInt32();

         //data
         return reader.ReadBytes(length);
      }

      public override Array MergeDictionary(Array dictionary, int[] indexes, Array data, int offset, int length)
      {
         throw new NotImplementedException();
      }

      public override Array PackDefinitions(Array data, int maxDefinitionLevel, out int[] definitions, out int definitionsLength, out int nullCount)
      {
         return PackDefinitions<byte[]>((byte[][])data, maxDefinitionLevel, out definitions, out definitionsLength, out nullCount);
      }

      public override Array UnpackDefinitions(Array src, int[] definitionLevels, int maxDefinitionLevel, out bool[] hasValueFlags)
      {
         return UnpackGenericDefinitions((byte[][])src, definitionLevels, maxDefinitionLevel, out hasValueFlags);
      }

      protected override void WriteOne(BinaryWriter writer, byte[] value)
      {
         writer.Write(value.Length);
         writer.Write(value);
      }
   }
}