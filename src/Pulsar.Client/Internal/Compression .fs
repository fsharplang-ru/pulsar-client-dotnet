namespace Pulsar.Client.Internal

open System.IO
open Pulsar.Client.Common
open ComponentAce.Compression.Libs.zlib
open K4os.Compression.LZ4
open Snappy
open ZstdNet
open System.IO.Compression

type internal CompressionCodec =
    { Encode: byte[] -> byte[]
      EncodeToStream: Stream -> byte[]-> unit
      Decode: int -> byte[] -> byte[]
      DecodeToStream: Stream -> byte[]-> unit }

module internal CompressionCodec =

    let encodeToStreamNone (stream : Stream) (bytes : byte[]) =
        stream.Write(bytes, 0, bytes.Length) |> ignore

    let decodeToStreamNone  = encodeToStreamNone

    let private zlib (bytes : byte[]) (createZLibStream : Stream -> ZOutputStream) (capacity : int) =
        use ms = new MemoryStream(capacity)
        use zlib = createZLibStream ms
        zlib.Write(bytes, 0, bytes.Length)
        zlib.finish()
        ms.ToArray()

    let private encodeZLib (bytes : byte[]) =
        let createZLibStream ms = new ZOutputStream(ms, zlibConst.Z_DEFAULT_COMPRESSION)
        createZLibStream |> zlib bytes <| 0

    let private encodeZLibToStream stream bytes =
        let zlib = new ZOutputStream(stream, zlibConst.Z_DEFAULT_COMPRESSION)
        zlib.Write(bytes, 0, bytes.Length)
        zlib.finish()

    let private decodeZLib (uncompressedSize : int) (bytes : byte[]) =
        let createZLibStream ms = new ZOutputStream(ms)
        createZLibStream |> zlib bytes <| uncompressedSize

    let private decodeZLibToStream stream bytes =
        let zlib = new ZOutputStream(stream)
        zlib.Write(bytes, 0, bytes.Length)
        zlib.finish()

    let private encodeLZ4 (bytes : byte[]) =
        let target = Array.zeroCreate<byte>(LZ4Codec.MaximumOutputSize(bytes.Length))
        let count = LZ4Codec.Encode(bytes, 0, bytes.Length, target, 0, target.Length)
        target |> Array.take count

    let private decodeLZ4 (uncompressedSize : int) (bytes : byte[]) =
        let target = Array.zeroCreate<byte>(uncompressedSize)
        LZ4Codec.Decode(bytes, 0, bytes.Length, target, 0, target.Length) |> ignore
        target

    let private encodeSnappy (bytes : byte[]) =
        bytes |> SnappyCodec.Compress

    let private encodeSnappyToStream stream bytes =
        let snappy = new SnappyStream(stream, CompressionMode.Compress, true)
        snappy.Write(bytes, 0, bytes.Length)
        //snappy.Flush |> ignore

    let private decodeSnappy (uncompressedSize : int) (bytes : byte[]) =
        let target = Array.zeroCreate<byte>(uncompressedSize)
        SnappyCodec.Uncompress(bytes, 0, bytes.Length, target, 0) |> ignore
        target

    let private encodeZStd (bytes : byte[]) =
        use zstd = new Compressor()
        bytes |> zstd.Wrap

    let private decodeZStd (uncompressedSize : int) (bytes : byte[]) =
        use zstd = new Decompressor()
        zstd.Unwrap(bytes, uncompressedSize)

    let create = function
        | CompressionType.ZLib -> { Encode = encodeZLib; EncodeToStream = encodeZLibToStream; Decode = decodeZLib; DecodeToStream = decodeZLibToStream }
        | CompressionType.LZ4 -> { Encode = encodeLZ4; EncodeToStream = (fun a b -> ()); Decode = decodeLZ4; DecodeToStream = fun a b -> () }
        | CompressionType.Snappy -> { Encode = encodeSnappy; EncodeToStream = encodeSnappyToStream; Decode = decodeSnappy; DecodeToStream = fun a b -> () }
        | CompressionType.ZStd -> { Encode = encodeZStd; EncodeToStream = (fun a b -> ()); Decode = decodeZStd; DecodeToStream = fun a b -> () }
        | CompressionType.None -> { Encode = id; EncodeToStream = encodeToStreamNone; Decode = (fun a b -> b); DecodeToStream = decodeToStreamNone }
        | _ as unknown -> raise(NotSupportedException <| sprintf "Compression codec '%A' not supported." unknown)