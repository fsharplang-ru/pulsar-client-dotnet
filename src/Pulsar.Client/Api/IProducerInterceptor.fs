﻿namespace Pulsar.Client.Api

open System
open Pulsar.Client.Common

type IProducerInterceptor<'T> =
    
    inherit IDisposable
    
    abstract member Eligible: message:MessageBuilder<'T> -> bool
    
    abstract member BeforeSend: producer:IProducer<'T> * message:MessageBuilder<'T> -> MessageBuilder<'T>  

    abstract member OnSendAcknowledgement: producer:IProducer<'T> * message:MessageBuilder<'T> * messageId:MessageId * ``exception``:Exception -> unit
