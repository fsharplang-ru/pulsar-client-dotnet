namespace Pulsar.Client.Api

open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Threading.Tasks
open FSharp.UMX
open pulsar.proto
open Pulsar.Client.Common
open Pulsar.Client.Internal
open System
open Microsoft.Extensions.Logging
open System.Collections.Generic
open System.Timers
open System.IO
open ProtoBuf

type ProducerImpl private (producerConfig: ProducerConfiguration, clientConfig: PulsarClientConfiguration, connectionPool: ConnectionPool,
                           partitionIndex: int, lookup: BinaryLookupService, cleanup: ProducerImpl -> unit) as this =
    let producerId = Generators.getNextProducerId()

    let prefix = sprintf "producer(%u, %s, %i)" %producerId producerConfig.ProducerName partitionIndex
    let producerCreatedTsc = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

    let pendingMessages = Queue<PendingMessage>()
    let batchItems = ResizeArray<BatchItem>()

    let compressionCodec = CompressionCodec.create producerConfig.CompressionType

    let compressionType =
        match producerConfig.CompressionType with
            | CompressionType.None -> pulsar.proto.CompressionType.None
            | CompressionType.ZLib -> pulsar.proto.CompressionType.Zlib
            | CompressionType.LZ4 -> pulsar.proto.CompressionType.Lz4
            | CompressionType.ZStd -> pulsar.proto.CompressionType.Zstd
            | CompressionType.Snappy -> pulsar.proto.CompressionType.Snappy
            | _ -> pulsar.proto.CompressionType.None

    let createProducerTimeout = DateTime.Now.Add(clientConfig.OperationTimeout)
    let sendTimeoutMs = producerConfig.SendTimeout.TotalMilliseconds
    let connectionHandler =
        ConnectionHandler(prefix,
                          connectionPool,
                          lookup,
                          producerConfig.Topic.CompleteTopicName,
                          (fun () -> this.Mb.Post(ProducerMessage.ConnectionOpened)),
                          (fun ex -> this.Mb.Post(ProducerMessage.ConnectionFailed ex)),
                          Backoff { BackoffConfig.Default with
                                        Initial = TimeSpan.FromMilliseconds(100.0)
                                        Max = TimeSpan.FromSeconds(60.0)
                                        MandatoryStop = TimeSpan.FromMilliseconds(Math.Max(100.0, sendTimeoutMs - 100.0))})


    let failPendingMessage msg (ex: exn) =
        match msg.Callback with
        | SingleCallback tsc -> tsc.SetException(ex)
        | BatchCallbacks tscs ->
            tscs
            |> Seq.iter (fun tcs -> tcs.SetException(ex))

    let failPendingMessages (ex: exn) =
        while pendingMessages.Count > 0 do
            let msg = pendingMessages.Dequeue()
            failPendingMessage msg ex

    let sendTimeoutTimer = new Timer()
    let startSendTimeoutTimer () =
        if sendTimeoutMs > 0.0 then
            sendTimeoutTimer.Interval <- sendTimeoutMs
            sendTimeoutTimer.AutoReset <- true
            sendTimeoutTimer.Elapsed.Add(fun _ -> this.Mb.Post TimeoutCheck)
            sendTimeoutTimer.Start()

    let batchTimer = new Timer()
    let startSendBatchTimer () =
        if producerConfig.BatchingEnabled && producerConfig.MaxBatchingPublishDelay <> TimeSpan.Zero then
            batchTimer.Interval <- producerConfig.MaxBatchingPublishDelay.TotalMilliseconds
            batchTimer.AutoReset <- true
            batchTimer.Elapsed.Add(fun _ -> this.Mb.Post SendBatchMessage)
            batchTimer.Start()

    let resendMessages () =
        if pendingMessages.Count > 0 then
            Log.Logger.LogInformation("{0} resending {1} pending messages", prefix, pendingMessages.Count)
            while pendingMessages.Count > 0 do
                let pendingMessage = pendingMessages.Dequeue()
                this.Mb.Post(SendMessage pendingMessage)
        else
            producerCreatedTsc.TrySetResult() |> ignore

    let verifyIfLocalBufferIsCorrupted (msg: PendingMessage) =
        task {
            use stream = MemoryStreamManager.GetStream()
            use reader = new BinaryReader(stream)
            do! msg.Payload (stream :> Stream) // materialize stream
            let streamSize = stream.Length
            stream.Seek(4L, SeekOrigin.Begin) |> ignore
            let cmdSize = reader.ReadInt32() |> int32FromBigEndian
            stream.Seek((10+cmdSize) |> int64, SeekOrigin.Begin) |> ignore
            let checkSum = reader.ReadInt32() |> int32FromBigEndian
            let checkSumPayload = (int streamSize) - 14 - cmdSize
            let computedCheckSum = CRC32C.Get(0u, stream, checkSumPayload) |> int32
            return checkSum <> computedCheckSum
        }

    let createMessageMetadata (message : byte[]) numMessagesInBatch =

        let metadata =
            MessageMetadata (
                SequenceId = %Generators.getNextSequenceId(),
                PublishTime = (DateTimeOffset.UtcNow.ToUnixTimeSeconds() |> uint64),
                ProducerName = producerConfig.ProducerName,
                UncompressedSize = (message.Length |> uint32),
                Compression = compressionType)

        let numMessages = match numMessagesInBatch with | Some(x) -> x | None -> 1

        if numMessagesInBatch.IsSome then
            metadata.NumMessagesInBatch <- numMessages

        metadata

    let stopProducer() =
        sendTimeoutTimer.Stop()
        batchTimer.Stop()
        cleanup(this)
        Log.Logger.LogInformation("{0} stopped", prefix)

    let mb = MailboxProcessor<ProducerMessage>.Start(fun inbox ->

        let rec loop () =
            async {
                let! msg = inbox.Receive()
                match msg with

                | ProducerMessage.ConnectionOpened ->

                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        Log.Logger.LogInformation("{0} starting register to topic {1}", prefix, producerConfig.Topic)
                        clientCnx.AddProducer producerId this.Mb
                        let requestId = Generators.getNextRequestId()
                        try
                            let payload = Commands.newProducer producerConfig.Topic.CompleteTopicName producerConfig.ProducerName producerId requestId
                            let! response = clientCnx.SendAndWaitForReply requestId payload |> Async.AwaitTask
                            let success = response |> PulsarResponseType.GetProducerSuccess
                            Log.Logger.LogInformation("{0} registered with name {1}", prefix, success.GeneratedProducerName)
                            connectionHandler.ResetBackoff()
                            resendMessages()
                        with
                        | ex ->
                            clientCnx.RemoveProducer producerId
                            Log.Logger.LogError(ex, "{0} Failed to create", prefix)
                            match ex with
                            | ProducerBlockedQuotaExceededException reason ->
                                Log.Logger.LogWarning("{0} Topic backlog quota exceeded. {1}", prefix, reason)
                                failPendingMessages(ex)
                            | ProducerBlockedQuotaExceededError reason ->
                                Log.Logger.LogWarning("{0} is blocked on creation because backlog exceeded. {1}", prefix, reason)
                            | _ ->
                                ()

                            match ex with
                            | TopicTerminatedException reason ->
                                connectionHandler.Terminate()
                                failPendingMessages(ex)
                                producerCreatedTsc.TrySetException(ex) |> ignore
                                stopProducer()
                            | _ when producerCreatedTsc.Task.IsCompleted || (connectionHandler.IsRetriableError ex && DateTime.Now < createProducerTimeout) ->
                                // Either we had already created the producer once (producerCreatedFuture.isDone()) or we are
                                // still within the initial timeout budget and we are dealing with a retriable error
                                connectionHandler.ReconnectLater ex
                            | _ ->
                                connectionHandler.Failed()
                                producerCreatedTsc.SetException(ex)
                                stopProducer()
                    | _ ->
                        Log.Logger.LogWarning("{0} connection opened but connection is not ready", prefix)
                    return! loop ()

                | ProducerMessage.ConnectionClosed clientCnx ->

                    Log.Logger.LogDebug("{0} ConnectionClosed", prefix)
                    let clientCnx = clientCnx :?> ClientCnx
                    connectionHandler.ConnectionClosed clientCnx
                    clientCnx.RemoveProducer(producerId)
                    return! loop ()

                | ProducerMessage.ConnectionFailed  ex ->

                    Log.Logger.LogDebug("{0} ConnectionFailed", prefix)
                    if (DateTime.Now > createProducerTimeout && producerCreatedTsc.TrySetException(ex)) then
                        Log.Logger.LogInformation("{0} creation failed", prefix)
                        connectionHandler.Failed()
                        stopProducer()
                    else
                        return! loop ()

                | ProducerMessage.StoreBatchItem (message, channel) ->

                    Log.Logger.LogDebug("{0} Begin store batch item. Batch container size: {1}", prefix, batchItems.Count)

                    let tcs = TaskCompletionSource(TaskContinuationOptions.RunContinuationsAsynchronously)
                    batchItems.Add({ Data = message; Tcs = tcs })

                    Log.Logger.LogDebug("{0} End store batch item. Batch container size: {1}", prefix, batchItems.Count)

                    if batchItems.Count = producerConfig.MaxMessagesPerBatch then
                        Log.Logger.LogDebug("{0} Max batch container size exceeded", prefix)
                        this.Mb.Post SendBatchMessage

                    channel.Reply(tcs)
                    return! loop ()

                | ProducerMessage.SendBatchMessage ->

                    let batchSize = batchItems.Count
                    if batchSize > 0 then
                        Log.Logger.LogDebug("{0} SendBatchMessage started", prefix)

                        use messageStream = MemoryStreamManager.GetStream()
                        use messageWriter = new BinaryWriter(messageStream)

                        for message in batchItems do
                            let smm = SingleMessageMetadata(PayloadSize = message.Data.Length)
                            Serializer.SerializeWithLengthPrefix(messageStream, smm, PrefixStyle.Fixed32BigEndian)
                            messageWriter.Write(message.Data)

                        let batchData = messageStream.ToArray()
                        let payloadF stream = batchData |> compressionCodec.EncodeToStream stream
                        let metadata = createMessageMetadata batchData (Some batchSize)
                        let sequenceId = %metadata.SequenceId
                        let payload = Commands.newSend producerId sequenceId batchSize metadata payloadF
                        let agentMessage = SendMessage {
                                SequenceId = sequenceId
                                Payload = payload
                                Callback = BatchCallbacks
                                    (batchItems
                                        |> Seq.map(fun i -> i.Tcs)
                                        |> Seq.toArray)
                                CreatedAt = DateTime.Now }
                        this.Mb.Post(agentMessage)
                        batchItems.Clear()
                        Log.Logger.LogDebug("{0} Pending batch created. Batch size: {1}", prefix, batchSize)

                    return! loop ()

                | ProducerMessage.BeginSendMessage (message, channel) ->

                    Log.Logger.LogDebug("{0} BeginSendMessage", prefix)
                    let metadata = createMessageMetadata message None
                    let payloadF stream = message |> compressionCodec.EncodeToStream stream
                    let sequenceId = %metadata.SequenceId
                    let payload = Commands.newSend producerId sequenceId 1 metadata payloadF
                    let tcs = TaskCompletionSource(TaskContinuationOptions.RunContinuationsAsynchronously)
                    this.Mb.Post(SendMessage { SequenceId = sequenceId; Payload = payload; Callback = SingleCallback tcs; CreatedAt = DateTime.Now })
                    channel.Reply(tcs)
                    return! loop ()

                | ProducerMessage.SendMessage pendingMessage ->

                    Log.Logger.LogDebug("{0} SendMessage id={1}", prefix, %pendingMessage.SequenceId)
                    if pendingMessages.Count <= producerConfig.MaxPendingMessages then
                        pendingMessages.Enqueue(pendingMessage)
                        match connectionHandler.ConnectionState with
                        | Ready clientCnx ->
                            let! success = clientCnx.Send pendingMessage.Payload
                            if success then
                                Log.Logger.LogDebug("{0} send complete", prefix)
                            else
                                Log.Logger.LogInformation("{0} send failed", prefix)
                        | _ ->
                            Log.Logger.LogWarning("{0} not connected, skipping send", prefix)
                    else
                        failPendingMessage pendingMessage (ProducerQueueIsFullError "Producer send queue is full.")
                    return! loop ()

                | ProducerMessage.SendReceipt receipt ->

                    let sequenceId = receipt.SequenceId
                    let pendingMessage = pendingMessages.Peek()
                    let expectedSequenceId = pendingMessage.SequenceId
                    if sequenceId > expectedSequenceId then
                        Log.Logger.LogWarning("{0} Got ack for msg {1}. expecting {2} - queue-size: {3}",
                            prefix, receipt, expectedSequenceId, pendingMessages.Count)
                        // Force connection closing so that messages can be re-transmitted in a new connection
                        match connectionHandler.ConnectionState with
                        | Ready clientCnx -> clientCnx.Close()
                        | _ -> ()
                    elif sequenceId < expectedSequenceId then
                        Log.Logger.LogInformation("{0} Got ack for timed out msg {1} last-seq: {2}",
                            prefix, receipt, expectedSequenceId)
                    else
                        Log.Logger.LogDebug("{0} Received ack for message {1}",
                            prefix, receipt)
                        pendingMessages.Dequeue() |> ignore
                        let msgId = { LedgerId = receipt.LedgerId; EntryId = receipt.EntryId; Partition = partitionIndex; Type = Individual; TopicName = %"" }
                        match pendingMessage.Callback with
                        | SingleCallback tcs ->
                            tcs.SetResult(msgId)
                        | BatchCallbacks tcss ->
                            tcss |> Array.iter (fun tcs -> tcs.SetResult(msgId))
                    return! loop ()

                | ProducerMessage.RecoverChecksumError sequenceId ->

                    //* Checks message checksum to retry if message was corrupted while sending to broker. Recomputes checksum of the
                    //* message header-payload again.
                    //* <ul>
                    //* <li><b>if matches with existing checksum</b>: it means message was corrupt while sending to broker. So, resend
                    //* message</li>
                    //* <li><b>if doesn't match with existing checksum</b>: it means message is already corrupt and can't retry again.
                    //* So, fail send-message by failing callback</li>
                    //* </ul>
                    Log.Logger.LogWarning("{0} RecoverChecksumError id={1}", prefix, sequenceId)
                    if pendingMessages.Count > 0 then
                        let pendingMessage = pendingMessages.Peek()
                        let expectedSequenceId = pendingMessage.SequenceId
                        if sequenceId = expectedSequenceId then
                            let! corrupted = verifyIfLocalBufferIsCorrupted pendingMessage |> Async.AwaitTask
                            if corrupted then
                                // remove message from pendingMessages queue and fail callback
                                pendingMessages.Dequeue() |> ignore
                                failPendingMessage pendingMessage (ChecksumException "Checksum failed on corrupt message")
                            else
                                Log.Logger.LogDebug("{0} Message is not corrupted, retry send-message with sequenceId {1}", prefix, sequenceId)
                                resendMessages()
                        else
                            Log.Logger.LogDebug("{0} Corrupt message is already timed out {1}", prefix, sequenceId)
                    else
                        Log.Logger.LogDebug("{0} Got send failure for timed out seqId {1}", prefix, sequenceId)
                    return! loop ()

                | ProducerMessage.Terminated ->

                    match connectionHandler.ConnectionState with
                    | Closed | Terminated -> ()
                    | _ ->
                        connectionHandler.Terminate()
                        failPendingMessages(TopicTerminatedException("The topic has been terminated"))
                    return! loop ()

                | ProducerMessage.TimeoutCheck ->

                    match connectionHandler.ConnectionState with
                    | Closed | Terminated -> ()
                    | _ ->
                        if pendingMessages.Count > 0 then
                            let firstMessage = pendingMessages.Peek()
                            if firstMessage.CreatedAt.AddMilliseconds(sendTimeoutMs) >= DateTime.Now then
                                // The diff is less than or equal to zero, meaning that the message has been timed out.
                                // Set the callback to timeout on every message, then clear the pending queue.
                                Log.Logger.LogInformation("{0} Message send timed out. Failing {1} messages", prefix, pendingMessages.Count)
                                let ex = TimeoutException "Could not send message to broker within given timeout"
                                failPendingMessages ex
                    return! loop ()

                | ProducerMessage.Close channel ->

                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        connectionHandler.Closing()
                        Log.Logger.LogInformation("{0} starting close", prefix)
                        let requestId = Generators.getNextRequestId()
                        let payload = Commands.newCloseProducer producerId requestId
                        task {
                            try
                                let! response = clientCnx.SendAndWaitForReply requestId payload |> Async.AwaitTask
                                response |> PulsarResponseType.GetEmpty
                                clientCnx.RemoveProducer(producerId)
                                connectionHandler.Closed()
                                stopProducer()
                                failPendingMessages(AlreadyClosedException("Producer was already closed"))
                            with
                            | ex ->
                                Log.Logger.LogError(ex, "{0} failed to close", prefix)
                                reraize ex
                        } |> channel.Reply
                    | _ ->
                        Log.Logger.LogInformation("{0} can't close since connection already closed", prefix)
                        connectionHandler.Closed()
                        stopProducer()
                        failPendingMessages(AlreadyClosedException("Producer was already closed"))
                        channel.Reply(Task.FromResult())

            }
        loop ()
    )

    do mb.Error.Add(fun ex -> Log.Logger.LogCritical(ex, "{0} mailbox failure", prefix))
    do startSendTimeoutTimer()
    do startSendBatchTimer()

    member private this.SendMessage message =
        if producerConfig.BatchingEnabled then
            mb.PostAndAsyncReply(fun channel -> StoreBatchItem (message, channel))
        else
            mb.PostAndAsyncReply(fun channel -> BeginSendMessage (message, channel))

    member private this.Mb with get(): MailboxProcessor<ProducerMessage> = mb

    override this.Equals producer =
        producerId = (producer :?> IProducer).ProducerId

    override this.GetHashCode () = int producerId

    member private this.InitInternal() =
       task {
           do connectionHandler.GrabCnx()
           return! producerCreatedTsc.Task
       }

    static member Init(producerConfig: ProducerConfiguration, clientConfig: PulsarClientConfiguration, connectionPool: ConnectionPool,
                       partitionIndex: int, lookup: BinaryLookupService, cleanup: ProducerImpl -> unit) =
        task {
            let producer = new ProducerImpl(producerConfig, clientConfig, connectionPool, partitionIndex, lookup, cleanup)
            do! producer.InitInternal()
            return producer :> IProducer
        }

    interface IProducer with

        member this.CloseAsync() =
            task {
                match connectionHandler.ConnectionState with
                | Closing | Closed ->
                    return ()
                | _ ->
                    let! result = mb.PostAndAsyncReply(ProducerMessage.Close)
                    return! result
            }

        member this.SendAndForgetAsync (message: byte[]) =
            task {
                connectionHandler.CheckIfActive()
                let! _ = this.SendMessage message
                return ()
            }

        member this.SendAsync (message: byte[]) =
            task {
                connectionHandler.CheckIfActive()
                let! tcs = this.SendMessage message
                return! tcs.Task
            }

        member this.ProducerId with get() = producerId