﻿module internal Pulsar.Client.Internal.Generators

open System.Threading
open Pulsar.Client.Common
open FSharp.UMX
open System

let mutable private requestId = 0L
let mutable private producerId = 0L
let mutable private consumerId = 0L
let mutable private clientCnxId = 0L

let getNextRequestId(): RequestId =
    % (uint64 <| Interlocked.Increment(&requestId))

let getNextProducerId(): ProducerId =
    % (uint64 <| Interlocked.Increment(&producerId))

let getNextConsumerId(): ConsumerId =
    % (uint64 <| Interlocked.Increment(&consumerId))

let getNextClientCnxId(): ClientCnxId =
    % (uint64 <| Interlocked.Increment(&clientCnxId))
    
let getRandomName(): string =
    Guid.NewGuid().ToString().Substring(0, 5)