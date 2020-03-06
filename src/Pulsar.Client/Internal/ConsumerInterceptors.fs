﻿namespace Pulsar.Client.Internal

open System
open Microsoft.Extensions.Logging
open Pulsar.Client.Api
open Pulsar.Client.Common

type internal ConsumerInterceptors(interceptors: IConsumerInterceptor array) =
     member this.Interceptors = interceptors
     static member Empty with get() = ConsumerInterceptors([||])
     
     member this.BeforeConsume (consumer: IConsumer, msg: Message) =
          let mutable interceptorMessage = msg         
          for interceptor in interceptors do
               try
                    interceptorMessage <- interceptor.BeforeConsume(consumer, interceptorMessage)
               with e ->
                    Log.Logger.LogWarning("Error executing interceptor beforeConsume callback topic: {0} consumerId: {1}", consumer.Topic, consumer.ConsumerId, e)
          interceptorMessage
        
     member this.OnAcknowledge (consumer: IConsumer, msgId: MessageId, exn: Exception) =
          for interceptor in interceptors do
               try
                    interceptor.OnAcknowledge(consumer, msgId, exn)
               with e ->
                    Log.Logger.LogWarning("Error executing interceptor OnAcknowledge callback", e);
                    
     member this.OnAcknowledgeCumulative (consumer: IConsumer, msgId: MessageId, exn: Exception)=
          for interceptor in interceptors do
               try
                    interceptor.OnAcknowledgeCumulative(consumer, msgId, exn)
               with e ->
                    Log.Logger.LogWarning("Error executing interceptor OnAcknowledgeCumulative callback", e);

     member this.OnNegativeAcksSend (consumer: IConsumer, msgId: MessageId, exn: Exception) =
          for interceptor in interceptors do
               try
                    interceptor.OnAcknowledgeCumulative(consumer, msgId, exn)
               with e ->
                    Log.Logger.LogWarning("Error executing interceptor OnNegativeAcksSend callback", e);
          
     member this.OnAckTimeoutSend (consumer: IConsumer, msgId: MessageId, exn: Exception)  =
          for interceptor in interceptors do
               try
                    interceptor.OnAcknowledgeCumulative(consumer, msgId, exn)
               with e ->
                    Log.Logger.LogWarning("Error executing interceptor OnAckTimeoutSend callback", e);
          
     member this.Close() =
          for interceptor in interceptors do
               try
                    interceptor.Close()
               with e ->
                    Log.Logger.LogWarning("Fail to close consumer interceptor", e);