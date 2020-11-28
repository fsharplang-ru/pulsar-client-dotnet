﻿namespace Pulsar.Client.Internal

open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
open Pulsar.Client.Common
open System
open System.Net
open Microsoft.Extensions.Logging

type internal BinaryLookupService (config: PulsarClientConfiguration, connectionPool: ConnectionPool) =

    let endPointResolver = EndPointResolver(config.ServiceAddresses)

    let resolveEndPoint() = endPointResolver.Resolve()

    member this.GetPartitionsForTopic (topicName: TopicName) =
        task {
            let! metadata = this.GetPartitionedTopicMetadata topicName.CompleteTopicName
            if metadata.Partitions > 0 then
                return Array.init metadata.Partitions topicName.GetPartition
            else
                return [| topicName |]
        }

    member private this.GetPartitionedTopicMetadata (endpoint, topicName, backoff: Backoff, remainingTimeMs) =
         async {
            try
                let! clientCnx = endpoint |> connectionPool.GetBrokerlessConnection |> Async.AwaitTask
                let requestId = Generators.getNextRequestId()
                let payload = Commands.newPartitionMetadataRequest topicName requestId
                let! response = clientCnx.SendAndWaitForReply requestId payload |> Async.AwaitTask
                return PulsarResponseType.GetPartitionedTopicMetadata response
            with Flatten ex ->
                let delay = Math.Min(backoff.Next(), remainingTimeMs)
                if delay <= 0 then
                    raise (TimeoutException "Could not GetPartitionedTopicMetadata within configured timeout.")
                Log.Logger.LogWarning(ex, "GetPartitionedTopicMetadata failed will retry in {0} ms", delay)
                do! Async.Sleep delay
                return! this.GetPartitionedTopicMetadata(endpoint, topicName, backoff, remainingTimeMs - delay)
        }
        
     member this.GetPartitionedTopicMetadata topicName =
        task {
            let backoff = Backoff { BackoffConfig.Default with
                                        Initial = TimeSpan.FromMilliseconds(100.0)
                                        MandatoryStop = (config.OperationTimeout + config.OperationTimeout)
                                        Max = TimeSpan.FromMinutes(1.0) }
            let! result = this.GetPartitionedTopicMetadata(resolveEndPoint(), topicName, backoff, int config.OperationTimeout.TotalMilliseconds)
            return result
        }

    member this.GetBroker(topicName: CompleteTopicName) =
        this.FindBroker(resolveEndPoint(), false, topicName, 0)

    member private this.FindBroker(endpoint: DnsEndPoint, autoritative: bool, topicName: CompleteTopicName,
                                   redirectCount: int) =
        task {
            if config.MaxLookupRedirects > 0 && redirectCount > config.MaxLookupRedirects then
                raise (LookupException <| "Too many redirects: " + string redirectCount)
            let! clientCnx = connectionPool.GetBrokerlessConnection endpoint
            let requestId = Generators.getNextRequestId()
            let payload = Commands.newLookup topicName requestId autoritative config.ListenerName
            let! response = clientCnx.SendAndWaitForReply requestId payload
            let lookupTopicResult = PulsarResponseType.GetLookupTopicResult response
            // (1) build response broker-address
            let uri =
                if config.UseTls then
                    Uri(lookupTopicResult.BrokerServiceUrlTls)
                else
                    Uri(lookupTopicResult.BrokerServiceUrl)

            let resultEndpoint = DnsEndPoint(uri.Host, uri.Port)
            // (2) redirect to given address if response is: redirect
            if lookupTopicResult.Redirect then
                Log.Logger.LogDebug("Redirecting to {0} topicName {1}", resultEndpoint, topicName)
                return! this.FindBroker(resultEndpoint, lookupTopicResult.Authoritative, topicName, redirectCount + 1)
            else
                // (3) received correct broker to connect
                return if lookupTopicResult.Proxy
                       then { LogicalAddress = LogicalAddress resultEndpoint; PhysicalAddress = PhysicalAddress endpoint }
                       else { LogicalAddress = LogicalAddress resultEndpoint; PhysicalAddress = PhysicalAddress resultEndpoint }
        }

    member this.GetTopicsUnderNamespace (ns : NamespaceName, isPersistent : bool) =
        task {
            let backoff = Backoff { BackoffConfig.Default with
                                        Initial = TimeSpan.FromMilliseconds(100.0)
                                        MandatoryStop = (config.OperationTimeout + config.OperationTimeout)
                                        Max = TimeSpan.FromMinutes(1.0) }
            let! result = this.GetTopicsUnderNamespace(resolveEndPoint(), ns, backoff, int config.OperationTimeout.TotalMilliseconds, isPersistent)
            return result
        }

    member private this.GetTopicsUnderNamespace (endpoint: DnsEndPoint, ns: NamespaceName, backoff: Backoff,
                                                 remainingTimeMs: int, isPersistent: bool) =
        async {
            try
                let! clientCnx = connectionPool.GetBrokerlessConnection endpoint |> Async.AwaitTask
                let requestId = Generators.getNextRequestId()
                let payload = Commands.newGetTopicsOfNamespaceRequest ns requestId isPersistent
                let! response = clientCnx.SendAndWaitForReply requestId payload |> Async.AwaitTask
                let result = PulsarResponseType.GetTopicsOfNamespace response
                return result
            with Flatten ex ->
                let delay = Math.Min(backoff.Next(), remainingTimeMs)
                if delay <= 0 then
                    raise (TimeoutException "Could not getTopicsUnderNamespace within configured timeout.")
                Log.Logger.LogWarning(ex, "GetTopicsUnderNamespace failed will retry in {0} ms", delay)
                do! Async.Sleep delay
                return! this.GetTopicsUnderNamespace(endpoint, ns, backoff, remainingTimeMs - delay, isPersistent)
        }
        
    member this.GetSchema(topicName: CompleteTopicName, ?schemaVersion: SchemaVersion) =
        task {
            let backoff = Backoff { BackoffConfig.Default with
                                        Initial = TimeSpan.FromMilliseconds(100.0)
                                        MandatoryStop = (config.OperationTimeout + config.OperationTimeout)
                                        Max = TimeSpan.FromMinutes(1.0) }
            let! result = this.GetSchema(resolveEndPoint(), topicName, schemaVersion, backoff, int config.OperationTimeout.TotalMilliseconds)
            return result
        }
        
    member private this.GetSchema(endpoint: DnsEndPoint, topicName: CompleteTopicName, schemaVersion: SchemaVersion option,
                                  backoff: Backoff, remainingTimeMs: int) =
        async {
            try
                let! clientCnx = connectionPool.GetBrokerlessConnection endpoint |> Async.AwaitTask
                let requestId = Generators.getNextRequestId()
                let payload = Commands.newGetSchema topicName requestId schemaVersion
                let! response = clientCnx.SendAndWaitForReply requestId payload |> Async.AwaitTask
                let schema = PulsarResponseType.GetTopicSchema response
                if schema.IsNone then
                    Log.Logger.LogWarning("No schema found for topic {0} version {1}", topicName, schemaVersion)
                return schema
            with Flatten ex ->
                let delay = Math.Min(backoff.Next(), remainingTimeMs)
                if delay <= 0 then
                    raise (TimeoutException "Could not GetSchema within configured timeout.")
                Log.Logger.LogWarning(ex, "GetSchema failed will retry in {0} ms", delay)
                do! Async.Sleep delay
                return! this.GetSchema(endpoint, topicName, schemaVersion, backoff, remainingTimeMs - delay)
        }