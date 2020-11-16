﻿namespace Pulsar.Client.Api

open System
open System.Threading
open System.Threading.Tasks
open Pulsar.Client.Common

type IConsumer<'T> =
    inherit IAsyncDisposable

    /// Receive a single message, wait asynchronously if no message is ready.
    abstract member ReceiveAsync: unit -> Task<Message<'T>>
    /// Receive a single message, wait asynchronously if no message is ready, can be cancelled.
    abstract member ReceiveAsync: CancellationToken -> Task<Message<'T>>
    /// Retrieves messages when has enough messages or wait timeout and completes with received messages.
    abstract member BatchReceiveAsync: unit -> Task<Messages<'T>>
    /// Retrieves messages when has enough messages or wait timeout and completes with received messages, can be cancelled.
    abstract member BatchReceiveAsync: CancellationToken -> Task<Messages<'T>>
    /// Asynchronously acknowledge the consumption of a single message
    abstract member AcknowledgeAsync: messageId:MessageId -> Task<unit>
    /// Asynchronously acknowledge the consumption of Messages
    abstract member AcknowledgeAsync: messages:Messages<'T> -> Task<unit>
    /// Acknowledge the reception of all the messages in the stream up to (and including) the provided message.
    abstract member AcknowledgeCumulativeAsync: messageId:MessageId -> Task<unit>
    /// Redelivers all the unacknowledged messages
    abstract member RedeliverUnacknowledgedMessagesAsync: unit -> Task<unit>
    /// Unsubscribes consumer
    abstract member UnsubscribeAsync: unit -> Task<unit>
    /// Return true if the topic was terminated and this consumer has already consumed all the messages in the topic.
    abstract member HasReachedEndOfTopic: bool
    /// Reset the subscription associated with this consumer to a specific message id.
    abstract member SeekAsync: messageId:MessageId -> Task<unit>
    /// Reset the subscription associated with this consumer to a specific message publish time (unix timestamp).
    abstract member SeekAsync: timestamp:uint64 -> Task<unit>
    /// Get the last message id available available for consume.
    abstract member GetLastMessageIdAsync: unit -> Task<MessageId>
    /// Acknowledge the failure to process a single message.
    abstract member NegativeAcknowledge: messageId:MessageId -> Task<unit>
    /// Acknowledge the failure to process Messages
    abstract member NegativeAcknowledge: messages:Messages<'T> -> Task<unit>
    /// Internal client consumer id
    abstract member ConsumerId: ConsumerId
    /// Get a topic for the consumer
    abstract member Topic: string
    /// Get the consumer name
    abstract member Name: string
    /// Get statistics for the consumer.
    abstract member GetStatsAsync: unit -> Task<ConsumerStats>
    /// ReconsumeLater the consumption of Message
    abstract member ReconsumeLaterAsync: message:Message<'T> * deliverAt:int64 -> Task<unit>
    /// ReconsumeLater the consumption of Messages
    abstract member ReconsumeLaterAsync: messages:Messages<'T> * deliverAt:int64 -> Task<unit>
    /// ReconsumeLater the reception of all the messages in the stream up to (and including) the provided message.
    abstract member ReconsumeLaterCumulativeAsync: message:Message<'T> * deliverAt:int64 -> Task<unit>