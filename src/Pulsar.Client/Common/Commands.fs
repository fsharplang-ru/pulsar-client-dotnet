module Pulsar.Client.Common.Commands

open pulsar.proto
open FSharp.UMX
open ProtoBuf
open System.IO
open System.Net
open Microsoft.Extensions.Logging
open Pulsar.Client.Internal

type internal CommandType = BaseCommand.Type

Serializer.PrepareSerializer<BaseCommand>()

let internal serializeSimpleCommand(command : BaseCommand) =
    fun (output: Stream) ->
        use stream = MemoryStreamManager.GetStream()

        // write fake totalLength
        for i in 1..4 do
            stream.WriteByte(0uy)

        // write commandPayload
        Serializer.SerializeWithLengthPrefix(stream, command, PrefixStyle.Fixed32BigEndian)
        let frameSize = int stream.Length

        let totalSize = frameSize - 4

        //write total size and command size
        stream.Seek(0L,SeekOrigin.Begin) |> ignore
        use binaryWriter = new BinaryWriter(stream)
        binaryWriter.Write(int32ToBigEndian totalSize)
        stream.Seek(0L, SeekOrigin.Begin) |> ignore

        Log.Logger.LogDebug("Sending message of type {0}", command.``type``)
        stream.CopyToAsync(output)

let internal serializePayloadCommand (command : BaseCommand) (metadata: MessageMetadata) (payloadF: Stream -> unit) =
    fun (output: Stream) ->
        use stream = MemoryStreamManager.GetStream()

        // write fake totalLength
        for i in 1..4 do
            stream.WriteByte(0uy)

        // write commandPayload
        Serializer.SerializeWithLengthPrefix(stream, command, PrefixStyle.Fixed32BigEndian)

        let stream1Size = int stream.Length

        // write magic number 0x0e01
        stream.WriteByte(14uy)
        stream.WriteByte(1uy)

        // write fake CRC sum and fake metadata length
        for i in 1..4 do
            stream.WriteByte(0uy)

        // write metadata
        Serializer.SerializeWithLengthPrefix(stream, metadata, PrefixStyle.Fixed32BigEndian)
        let stream2Size = int stream.Length
        let totalMetadataSize = stream2Size - stream1Size - 6

        // write payload
        payloadF stream

        let frameSize = int stream.Length
        let totalSize = frameSize - 4
        let payloadSize = frameSize - stream2Size

        let crcStart = stream1Size + 2
        let crcPayloadStart = crcStart + 4

        // write missing sizes
        use binaryWriter = new BinaryWriter(stream)

        //write CRC
        stream.Seek(int64 crcPayloadStart, SeekOrigin.Begin) |> ignore
        let crc = int32 <| CRC32C.Get(0u, stream, totalMetadataSize + payloadSize)
        stream.Seek(int64 crcStart, SeekOrigin.Begin) |> ignore
        binaryWriter.Write(int32ToBigEndian crc)

        //write total size and command size
        stream.Seek(0L, SeekOrigin.Begin) |> ignore
        binaryWriter.Write(int32ToBigEndian totalSize)

        stream.Seek(0L, SeekOrigin.Begin) |> ignore

        Log.Logger.LogDebug("Sending message of type {0}", command.``type``)
        stream.CopyToAsync(output)

let newPartitionMetadataRequest(topicName : CompleteTopicName) (requestId : RequestId) : Payload =
    let request = CommandPartitionedTopicMetadata(Topic = %topicName, RequestId = %requestId)
    let command = BaseCommand(``type`` = CommandType.PartitionedMetadata, partitionMetadata = request)
    serializeSimpleCommand command

let newSend (producerId : ProducerId) (sequenceId : SequenceId) (numMessages : int) (msgMetadata : MessageMetadata) (payloadF: Stream -> unit) : Payload =
    let request = CommandSend(ProducerId = %producerId, SequenceId = %sequenceId, NumMessages = numMessages)
    let command = BaseCommand(``type`` = CommandType.Send, Send = request)
    serializePayloadCommand command msgMetadata payloadF

let newAck (consumerId : ConsumerId) (messageId: MessageId) (ackType : AckType) : Payload =
    let request = CommandAck(ConsumerId = %consumerId, ack_type = ackType.ToCommandAckType())
    request.MessageIds.Add(messageId.ToMessageIdData())
    let command = BaseCommand(``type`` = CommandType.Ack, Ack = request)
    serializeSimpleCommand command

let newMultiMessageAck (consumerId : ConsumerId) (messages: MessageId seq) : Payload =
    let request = CommandAck(ConsumerId = %consumerId, ack_type = CommandAck.AckType.Individual)
    messages
    |> Seq.map (fun messageId -> messageId.ToMessageIdData())
    |> request.MessageIds.AddRange
    let command = BaseCommand(``type`` = CommandType.Ack, Ack = request)
    serializeSimpleCommand command

let newConnect (clientVersion: string) (protocolVersion: ProtocolVersion) (proxyToBroker: Option<DnsEndPoint>) : Payload =
    let request = CommandConnect(ClientVersion = clientVersion, ProtocolVersion = (int) protocolVersion)
    match proxyToBroker with
    | Some logicalAddress -> request.ProxyToBrokerUrl <- sprintf "%s:%d" logicalAddress.Host logicalAddress.Port
    | None -> ()
    let command = BaseCommand(``type`` = CommandType.Connect, Connect = request)
    command |> serializeSimpleCommand

let newPong () : Payload =
    let request = CommandPong()
    let command = BaseCommand(``type`` = CommandType.Pong, Pong = request)
    command |> serializeSimpleCommand

let newLookup (topicName : CompleteTopicName) (requestId : RequestId) (authoritative : bool) =
    let request = CommandLookupTopic(Topic = %topicName, Authoritative = authoritative, RequestId = uint64(%requestId))
    let command = BaseCommand(``type`` = CommandType.Lookup, lookupTopic = request)
    command |> serializeSimpleCommand

let newProducer (topicName : CompleteTopicName) (producerName: string) (producerId : ProducerId) (requestId : RequestId) =
    let request = CommandProducer(Topic = %topicName, ProducerId = %producerId, RequestId = %requestId, ProducerName = producerName)
    let command = BaseCommand(``type`` = CommandType.Producer, Producer = request)
    command |> serializeSimpleCommand

let newGetTopicsOfNamespaceRequest (ns : NamespaceName) (requestId : RequestId) (mode : TopicDomain) =
    let mode =
        match mode with
        | Persistent -> CommandGetTopicsOfNamespace.Mode.Persistent
        | NonPersistent -> CommandGetTopicsOfNamespace.Mode.NonPersistent
    let request = CommandGetTopicsOfNamespace(Namespace = ns.ToString(), RequestId = uint64(%requestId), mode = mode)
    let command = BaseCommand(``type`` = CommandType.GetTopicsOfNamespace, getTopicsOfNamespace = request)
    command |> serializeSimpleCommand

let newSubscribe (topicName: CompleteTopicName) (subscription: string) (consumerId: ConsumerId) (requestId: RequestId)
    (consumerName: string) (subscriptionType: SubscriptionType) (subscriptionInitialPosition: SubscriptionInitialPosition) =
    let subType =
        match subscriptionType with
        | SubscriptionType.Exclusive -> CommandSubscribe.SubType.Exclusive
        | SubscriptionType.Shared -> CommandSubscribe.SubType.Shared
        | SubscriptionType.Failover -> CommandSubscribe.SubType.Failover
        | _ -> failwith "Unknown subscription type"

    let initialPosition =
        match subscriptionInitialPosition with
        | SubscriptionInitialPosition.Earliest -> CommandSubscribe.InitialPosition.Earliest
        | SubscriptionInitialPosition.Latest -> CommandSubscribe.InitialPosition.Latest
        | _ -> failwith "Unknown initialPosition type"

    let request = CommandSubscribe(Topic = %topicName, Subscription = subscription, subType = subType, ConsumerId = %consumerId,
                    RequestId = %requestId, ConsumerName =  consumerName, initialPosition = initialPosition)
    let command = BaseCommand(``type`` = CommandType.Subscribe, Subscribe = request)
    command |> serializeSimpleCommand

let newFlow (consumerId: ConsumerId) (messagePermits: int) =
    let request = CommandFlow(ConsumerId = %consumerId, messagePermits = (uint32 messagePermits))
    let command = BaseCommand(``type`` = CommandType.Flow, Flow = request)
    command |> serializeSimpleCommand

let newCloseConsumer (consumerId: ConsumerId) (requestId : RequestId) =
    let request = CommandCloseConsumer(ConsumerId = %consumerId, RequestId = %requestId)
    let command = BaseCommand(``type`` = CommandType.CloseConsumer, CloseConsumer = request)
    command |> serializeSimpleCommand

let newUnsubscribeConsumer (consumerId: ConsumerId) (requestId : RequestId) =
    let request = CommandUnsubscribe(ConsumerId = %consumerId, RequestId = %requestId)
    let command = BaseCommand(``type`` = CommandType.Unsubscribe, Unsubscribe = request)
    command |> serializeSimpleCommand

let newCloseProducer (producerId: ProducerId) (requestId : RequestId) =
    let request = CommandCloseProducer(ProducerId = %producerId, RequestId = %requestId)
    let command = BaseCommand(``type`` = CommandType.CloseProducer, CloseProducer = request)
    command |> serializeSimpleCommand

let newRedeliverUnacknowledgedMessages (consumerId: ConsumerId) (messageIds : Option<#seq<MessageId>>) =
    let request = CommandRedeliverUnacknowledgedMessages(ConsumerId = %consumerId)
    match messageIds with
    | Some ids -> ids |> Seq.iter (fun msgId ->
        Log.Logger.LogDebug("{0} should be redelivered", msgId)
        request.MessageIds.Add(msgId.ToMessageIdData()))
    | None -> ()

    let command = BaseCommand(``type`` = CommandType.RedeliverUnacknowledgedMessages, redeliverUnacknowledgedMessages = request)
    command |> serializeSimpleCommand