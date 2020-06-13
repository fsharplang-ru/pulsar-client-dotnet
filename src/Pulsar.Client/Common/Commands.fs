﻿module internal Pulsar.Client.Common.Commands

open pulsar.proto
open FSharp.UMX
open System
open System.Data
open ProtoBuf
open System.IO
open System.Net
open Microsoft.Extensions.Logging
open Pulsar.Client.Internal
open Pulsar.Client
open Pulsar.Client.Api
open Pulsar.Client.Common

type CommandType = BaseCommand.Type

[<Literal>]
let DEFAULT_MAX_MESSAGE_SIZE = 5_242_880 //5 * 1024 * 1024

let serializeSimpleCommand(command : BaseCommand) =
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

let serializePayloadCommand (command : BaseCommand) (metadata: MessageMetadata) (payload: byte[]) =
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
        stream.Write(payload, 0, payload.Length)

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

let newSend (producerId : ProducerId) (sequenceId : SequenceId) (highestSequenceId: SequenceId option)
    (numMessages : int) (msgMetadata : MessageMetadata) (payload: byte[]) : Payload =
    let request = CommandSend(ProducerId = %producerId, SequenceId = uint64 %sequenceId)
    if numMessages > 1 then
        request.NumMessages <- numMessages
    if highestSequenceId.IsSome then
        request.HighestSequenceId <- uint64 %highestSequenceId.Value
    let command = BaseCommand(``type`` = CommandType.Send, Send = request)
    serializePayloadCommand command msgMetadata payload

let newAck (consumerId : ConsumerId) (ledgerId: LedgerId) (entryId: EntryId) (ackType : AckType) : Payload =
    let request = CommandAck(ConsumerId = %consumerId, ack_type = ackType.ToCommandAckType())
    request.MessageIds.Add(MessageIdData(ledgerId = uint64 %ledgerId, entryId = uint64 %entryId))
    let command = BaseCommand(``type`` = CommandType.Ack, Ack = request)
    serializeSimpleCommand command

let newErrorAck (consumerId : ConsumerId) (ledgerId: LedgerId) (entryId: EntryId) (ackType : AckType)
    (error: CommandAck.ValidationError) : Payload =
    let request = CommandAck(ConsumerId = %consumerId, ack_type = ackType.ToCommandAckType())
    request.MessageIds.Add(MessageIdData(ledgerId = uint64 %ledgerId, entryId = uint64 %entryId))
    request.validation_error <- error
    let command = BaseCommand(``type`` = CommandType.Ack, Ack = request)
    serializeSimpleCommand command

let newMultiMessageAck (consumerId : ConsumerId) (messages: seq<LedgerId*EntryId>) : Payload =
    let request = CommandAck(ConsumerId = %consumerId, ack_type = CommandAck.AckType.Individual)
    messages
    |> Seq.map (fun (ledgerId, entryId) -> MessageIdData(ledgerId = uint64 %ledgerId, entryId = uint64 %entryId))
    |> request.MessageIds.AddRange
    let command = BaseCommand(``type`` = CommandType.Ack, Ack = request)
    serializeSimpleCommand command

let newConnect (authMethodName: string) (authData: Common.AuthData) (clientVersion: string) (protocolVersion: ProtocolVersion) (proxyToBroker: Option<DnsEndPoint>) : Payload =
    let request = CommandConnect(ClientVersion = clientVersion, ProtocolVersion = (int) protocolVersion, AuthMethodName = authMethodName)
    if authMethodName = "ycav1" then
        request.AuthMethod <- AuthMethod.AuthMethodYcaV1
    if authData.Bytes.Length > 0 then
        request.AuthData <- authData.Bytes
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

let newProducer (topicName : CompleteTopicName) (producerName: string) (producerId : ProducerId) (requestId : RequestId)
                (schemaInfo: SchemaInfo) (epoch: uint64) =
    let schema = getProtoSchema schemaInfo
    let request = CommandProducer(Topic = %topicName, ProducerId = %producerId, RequestId = %requestId, ProducerName = producerName,
                                  Epoch = epoch)
    if schema.``type`` <> Schema.Type.None then
        request.Schema <- schema
    let command = BaseCommand(``type`` = CommandType.Producer, Producer = request)
    command |> serializeSimpleCommand

let newSeekByMsgId (consumerId: ConsumerId) (requestId : RequestId) (messageId: MessageId) =
    let request =
        CommandSeek(
            ConsumerId = %consumerId, RequestId = %requestId,
            MessageId = MessageIdData(ledgerId = uint64(%messageId.LedgerId), entryId = uint64(%messageId.EntryId))
        )
    let command = BaseCommand(``type`` = CommandType.Seek, Seek = request)
    command |> serializeSimpleCommand

let newSeekByTimestamp (consumerId: ConsumerId) (requestId : RequestId) (timestamp: uint64) =
    let request =
        CommandSeek(
            ConsumerId = %consumerId, RequestId = %requestId, MessagePublishTime = timestamp
        )
    let command = BaseCommand(``type`` = CommandType.Seek, Seek = request)
    command |> serializeSimpleCommand

let newGetTopicsOfNamespaceRequest (ns : NamespaceName) (requestId : RequestId) (isPersistent : bool) =
    let mode =
        match isPersistent with
        | true -> CommandGetTopicsOfNamespace.Mode.Persistent
        | false -> CommandGetTopicsOfNamespace.Mode.NonPersistent
    let request = CommandGetTopicsOfNamespace(Namespace = ns.ToString(), RequestId = uint64(%requestId), mode = mode)
    let command = BaseCommand(``type`` = CommandType.GetTopicsOfNamespace, getTopicsOfNamespace = request)
    command |> serializeSimpleCommand

let newSubscribe (topicName: CompleteTopicName) (subscription: string) (consumerId: ConsumerId) (requestId: RequestId)
    (consumerName: string) (subscriptionType: SubscriptionType) (subscriptionInitialPosition: SubscriptionInitialPosition)
    (readCompacted: bool) (startMessageId: MessageIdData) (durable: bool) (startMessageRollbackDuration: TimeSpan)
    (createTopicIfDoesNotExist: bool) (keySharedPolicy: KeySharedPolicy option) (schemaInfo: SchemaInfo) =
    let schema = getProtoSchema schemaInfo
    let subType =
        match subscriptionType with
        | SubscriptionType.Exclusive -> CommandSubscribe.SubType.Exclusive
        | SubscriptionType.Shared -> CommandSubscribe.SubType.Shared
        | SubscriptionType.Failover -> CommandSubscribe.SubType.Failover
        | SubscriptionType.KeyShared -> CommandSubscribe.SubType.KeyShared
        | _ -> failwith "Unknown subscription type"
    let initialPosition =
        match subscriptionInitialPosition with
        | SubscriptionInitialPosition.Earliest -> CommandSubscribe.InitialPosition.Earliest
        | SubscriptionInitialPosition.Latest -> CommandSubscribe.InitialPosition.Latest
        | _ -> failwith "Unknown initialPosition type"
    let request = CommandSubscribe(Topic = %topicName, Subscription = subscription, subType = subType, ConsumerId = %consumerId,
                    RequestId = %requestId, ConsumerName =  consumerName, initialPosition = initialPosition, ReadCompacted = readCompacted,
                    StartMessageId = startMessageId, Durable = durable, ForceTopicCreation = createTopicIfDoesNotExist)
    match keySharedPolicy with
    | Some keySharedPolicy ->
        let meta = KeySharedMeta()
        match keySharedPolicy with
        | :? KeySharedPolicyAutoSplit ->
            meta.keySharedMode <- KeySharedMode.AutoSplit            
        | :? KeySharedPolicySticky as policy ->
            meta.keySharedMode <- KeySharedMode.Sticky
            for range in policy.Ranges do
                meta.hashRanges.Add(IntRange(Start = range.Start, End = range.End))
        | _ -> failwith "Unknown keySharedPolicy"
        request.keySharedMeta <- meta
    | None ->
        ()
    if startMessageRollbackDuration > TimeSpan.Zero then
        request.StartMessageRollbackDurationSec <- (startMessageRollbackDuration.TotalSeconds |> uint64)
    if schema.``type`` <> Schema.Type.None then
        request.Schema <- schema
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

let newRedeliverUnacknowledgedMessages (consumerId: ConsumerId) (messageIds : Option<seq<MessageIdData>>) =
    let request = CommandRedeliverUnacknowledgedMessages(ConsumerId = %consumerId)
    match messageIds with
    | Some ids -> ids |> Seq.iter (fun msgIdData -> request.MessageIds.Add(msgIdData))
    | None -> ()
    let command = BaseCommand(``type`` = CommandType.RedeliverUnacknowledgedMessages, redeliverUnacknowledgedMessages = request)
    command |> serializeSimpleCommand

let newGetLastMessageId (consumerId: ConsumerId) (requestId: RequestId) =
    let request = CommandGetLastMessageId(ConsumerId = %consumerId, RequestId = %requestId)
    let command = BaseCommand(``type`` = CommandType.GetLastMessageId, getLastMessageId = request)
    command |> serializeSimpleCommand
    
let newGetSchema (topicName: CompleteTopicName) (requestId : RequestId) (schemaVersion: Option<SchemaVersion>) =
    let request = CommandGetSchema(Topic = %topicName, RequestId = %requestId)
    match schemaVersion with
    | Some (SchemaVersion sv) -> request.SchemaVersion <- sv
    | None -> ()        
    let command = BaseCommand(``type`` = CommandType.GetSchema, getSchema = request)
    command |> serializeSimpleCommand