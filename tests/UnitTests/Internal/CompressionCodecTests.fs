module Pulsar.Client.UnitTests.Internal.CompressionCodecTests

open Pulsar.Client.Common
open Pulsar.Client.Internal
open Expecto
open Expecto.Flip
open System.Text
open System.IO

[<Tests>]
let tests =

    let createCodec codecType = codecType |> CompressionCodec.create

    let encoding = Encoding.UTF8

    let getBytes (message : string) = message |> encoding.GetBytes

    let getString (bytes : byte[]) = bytes |> encoding.GetString

    let hello = "Hello"
    let helloNone = hello |> getBytes
    let helloZLib = System.Convert.FromBase64String "eJzzSM3JyQcABYwB9Q=="
    let helloLZ4 = System.Convert.FromBase64String "UEhlbGxv"
    let helloSnappy = System.Convert.FromBase64String "BRBIZWxsbw=="
    let helloZStd = System.Convert.FromBase64String "KLUv/SAFKQAASGVsbG8="

    let testEncode compressionType expectedBytes =
        let codec = compressionType |> createCodec
        let encoded = hello |> getBytes |> codec.Encode
        Expect.isTrue "" (encoded = expectedBytes)

    let testDecode compressionType encodedBytes =
        let uncompressedSize = helloNone.Length
        let codec = compressionType |> createCodec
        let decoded = encodedBytes |> codec.Decode uncompressedSize |> getString
        decoded |> Expect.equal "" hello

    let testEncodeToStream compressionType expectedBytes =
        let codec = compressionType |> createCodec
        use ms = new MemoryStream()
        hello |> getBytes |> codec.EncodeToStream (ms :> Stream)
        ms.Seek(0L, SeekOrigin.Begin) |> ignore
        let encoded = ms.ToArray()
        Expect.isTrue "" (encoded = expectedBytes)

    let testDecodeToStream compressionType encodedBytes =
        let codec = compressionType |> createCodec
        use ms = new MemoryStream()
        encodedBytes |> codec.DecodeToStream (ms :> Stream)
        ms.Seek(0L, SeekOrigin.Begin) |> ignore
        let decoded = ms.ToArray()
        decoded |> getString |> Expect.equal "" hello

    testList "CompressionCodec" [

        test "None encoding returns same data" {
            helloNone |> testEncode CompressionType.None
            helloNone |> testEncodeToStream CompressionType.None
        }

        test "None decoding returns same data" {
            helloNone |> testDecode CompressionType.None
            helloNone |> testDecodeToStream CompressionType.None
        }

        test "Codec should make ZLib encoding" {
            helloZLib |> testEncode CompressionType.ZLib
            helloZLib |> testEncodeToStream CompressionType.ZLib
        }

        test "Codec should make ZLib decoding" {
            helloZLib |> testDecode CompressionType.ZLib
            helloZLib |> testDecodeToStream CompressionType.ZLib
        }

        test "Codec should make LZ4 encoding" {
            helloLZ4 |> testEncode CompressionType.LZ4
        }

        test "Codec should make LZ4 decoding" {
            helloLZ4 |> testDecode CompressionType.LZ4
        }

        test "Codec should make Snappy encoding" {
            helloSnappy |> testEncode CompressionType.Snappy
        }

        test "Codec should make Snappy decoding" {
            helloSnappy |> testDecode CompressionType.Snappy
        }

        test "Codec should make ZStd encoding" {
            helloZStd |> testEncode CompressionType.ZStd
        }

        test "Codec should make ZStd decoding" {
            helloZStd |> testDecode CompressionType.ZStd
        }
    ]