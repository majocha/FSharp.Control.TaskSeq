namespace FSharp.Control

open System
open System.Threading
open System.Threading.Tasks
open System.Threading.Tasks.Sources
open System.Runtime.CompilerServices
open System.Collections.Generic

open FSharp.Core.CompilerServices

/// <summary>
/// Represents a task sequence and is the output of using the <paramref name="taskSeq{...}" />
/// computation expression from this library. It is an alias for <see cref="T:System.IAsyncEnumerable&lt;_>" />.
/// </summary>
type TaskSeq<'T> = IAsyncEnumerable<'T>

/// <summary>
/// Marker exception used to unwind the producer when the enumerator is disposed
/// before the sequence completed. It is caught by the producer wrapper and never
/// escapes the library. For use by this library only, should not be used directly in user code.
/// </summary>
[<NoComparison; NoEquality>]
type TaskSeqDisposal =
    inherit Exception
    new: unit -> TaskSeqDisposal

/// <summary>
/// A one-shot awaitable signal used for the producer/consumer handshake,
/// based on <see cref="T:System.Threading.Tasks.Sources.ManualResetValueTaskSourceCore&lt;_>" />.
/// For use by this library only, should not be used directly in user code.
/// </summary>
[<NoComparison; NoEquality>]
type TaskSeqSignal<'T> =
    interface IValueTaskSource<'T>

    new: ('T -> 'T) -> TaskSeqSignal<'T>

    member WaitAsync: unit -> ValueTask<'T>
    member SetResult: value: 'T -> unit
    member SetException: error: exn -> unit
    member Reset: unit -> unit
    member Version: int16

/// <summary>
/// State shared between the producer (the <c>taskSeq</c> computation) and the consumer
/// (the <see cref="T:System.Collections.Generic.IAsyncEnumerator&lt;_>" />) of a task sequence.
/// For use by this library only, should not be used directly in user code.
/// </summary>
[<NoComparison; NoEquality>]
type TaskSeqState<'T> =

    {
        /// Consumer -> Producer: set by MoveNextAsync to request the next item.
        MoveNextRequest: TaskSeqSignal<unit>
        /// Producer -> Consumer: completed with true (item available in Current), false (end of
        /// sequence), or an exception.
        ItemResponse: TaskSeqSignal<bool>
        CancellationToken: CancellationToken
        /// Used by the IAsyncEnumerator interface to return the Current value.
        mutable Current: ValueOption<'T>
        /// Set by DisposeAsync to unwind the producer, running pending compensations.
        mutable DisposalRequested: bool
        KickOff: bool
    }

/// <summary>
/// Helpers operating on <see cref="TaskSeqState&lt;_&gt;" />. These are deliberately not inline:
/// they are used from inline builder member bodies, and keeping the publish/check operations
/// behind ordinary function calls keeps the inline fragments in a shape the runtime-async
/// compiler analysis accepts.
/// For use by this library only, should not be used directly in user code.
/// </summary>
module TaskSeqState =

    /// Publish an item to the consumer: sets Current and completes the response signal.
    val publishItem: state: TaskSeqState<'T> -> item: 'T -> unit
    /// Signal the end of the sequence.
    val publishCompleted: state: TaskSeqState<'T> -> unit
    /// Signal a producer failure; the consumer rethrows the original exception, unwrapped.
    val publishFaulted: state: TaskSeqState<'T> -> error: exn -> unit

    /// Raise the disposal sentinel exception.
    val raiseDisposalRequested: unit -> 'a

    /// Reset the request signal after the consumer asked for the next item,
    /// and raise the disposal sentinel if the enumerator was disposed in the meantime.
    val resetAfterMoveNextRequest: state: TaskSeqState<'T> -> unit

/// <summary>
/// The body of a <c>taskSeq</c> computation: a function that runs the computation against the
/// shared producer/consumer state. Values of this type only occur inline, inside a runtime-async
/// producer method. For use by this library only, should not be used directly in user code.
/// </summary>
type TaskSeqCode<'T> = TaskSeqState<'T> -> unit

/// <summary>
/// The producer side of a task sequence. Implements <see cref="T:System.Collections.Generic.IAsyncEnumerable&lt;_>" />
/// by starting the producer computation for each new enumerator.
/// For use by this library only, should not be used directly in user code.
/// </summary>
[<Struct; NoComparison; NoEquality>]
type TaskSeqEnumerable<'T> =
    interface IAsyncEnumerable<'T>

    new: runProducer: (TaskSeqState<'T> -> Task<unit>) -> TaskSeqEnumerable<'T>

    member RunProducer: state: TaskSeqState<'T> -> Task<unit>

/// <summary>
/// Main builder class for the <see cref="taskSeq" /> computation expression, implemented
/// with runtime-async compiler intrinsics.
/// </summary>
[<Class>]
type TaskSeqBuilder =

    member inline Delay: generator: (unit -> TaskSeqCode<'T>) -> TaskSeqCode<'T>
    member inline Run: code: TaskSeqCode<'T> -> TaskSeq<'T>
    member inline Zero: unit -> TaskSeqCode<'T>
    member inline ReturnFrom: task: Task -> TaskSeqCode<'T>
    member inline ReturnFrom: task: Task<'U> -> TaskSeqCode<'T>
    member inline ReturnFrom: task: ValueTask -> TaskSeqCode<'T>
    member inline ReturnFrom: task: ValueTask<'U> -> TaskSeqCode<'T>
    member inline ReturnFrom: computation: Async<'U> -> TaskSeqCode<'T>

    member inline Bind: task: Task<'U> * continuation: ('U -> TaskSeqCode<'T>) -> TaskSeqCode<'T>
    member inline Bind: task: ValueTask<'U> * continuation: ('U -> TaskSeqCode<'T>) -> TaskSeqCode<'T>
    member inline Bind: computation: Async<'U> * continuation: ('U -> TaskSeqCode<'T>) -> TaskSeqCode<'T>

    member inline Combine: task1: TaskSeqCode<'T> * task2: TaskSeqCode<'T> -> TaskSeqCode<'T>
    member inline TryFinally: body: TaskSeqCode<'T> * compensationAction: (unit -> unit) -> TaskSeqCode<'T>
    member inline TryFinallyAsync: body: TaskSeqCode<'T> * compensationAction: (unit -> Task) -> TaskSeqCode<'T>
    member inline TryWith: body: TaskSeqCode<'T> * catch: (exn -> TaskSeqCode<'T>) -> TaskSeqCode<'T>

    member inline Using: resource: 'Resource * body: ('Resource -> TaskSeqCode<'T>) -> TaskSeqCode<'T>

    member inline While: condition: (unit -> bool) * body: TaskSeqCode<'T> -> TaskSeqCode<'T>
    /// Used by `For`. Unclear if `while!` (from F# 8.0) hits this
    member inline WhileAsync: condition: (unit -> ValueTask<bool>) * body: TaskSeqCode<'T> -> TaskSeqCode<'T>
    member inline For: sequence: seq<'TElement> * body: ('TElement -> TaskSeqCode<'T>) -> TaskSeqCode<'T>
    member inline For: source: #TaskSeq<'TElement> * body: ('TElement -> TaskSeqCode<'T>) -> TaskSeqCode<'T>
    member inline Yield: value: 'T -> TaskSeqCode<'T>
    member inline YieldFrom: source: seq<'T> -> TaskSeqCode<'T>
    member inline YieldFrom: source: #TaskSeq<'T> -> TaskSeqCode<'T>

/// <summary>
/// Contains extension methods for the main builder class for the <see cref="taskSeq" /> computation expression.
/// This module is not meant to be accessed directly from user code.
/// </summary>
[<AutoOpen>]
module TaskSeqAwaitableExtensions =

    type TaskSeqBuilder with

        /// <summary>Lowest-priority SRTP fallback for binding custom awaitables (task-like types).</summary>
        [<NoEagerConstraintApplication>]
        member inline Bind< ^TaskLike, 'T, 'U, ^Awaiter> :
            task: ^TaskLike * continuation: ('T -> TaskSeqCode<'U>) -> TaskSeqCode<'U>
                when ^TaskLike: (member GetAwaiter: unit -> ^Awaiter)
                and ^Awaiter :> ICriticalNotifyCompletion
                and ^Awaiter: (member get_IsCompleted: unit -> bool)
                and ^Awaiter: (member GetResult: unit -> 'T)

[<AutoOpen>]
module TaskSeqBuilder =

    /// <summary>
    /// Builds an asynchronous task sequence based on <see cref="IAsyncEnumerable&lt;'T&gt;" /> using computation expression syntax.
    /// </summary>
    val taskSeq: TaskSeqBuilder

/// <summary>
/// Builder class for the <see cref="taskSeqDynamic" /> computation expression. Inherits all members
/// from <see cref="TaskSeqBuilder" />. With the runtime-async implementation there is no separate
/// dynamic (FSI) code path; the same builder works in both compiled and interpreted scenarios.
/// </summary>
[<Class>]
type TaskSeqDynamicBuilder =
    inherit TaskSeqBuilder
    new: unit -> TaskSeqDynamicBuilder

[<AutoOpen>]
module TaskSeqDynamicBuilder =

    /// <summary>
    /// Builds an asynchronous task sequence. Historically this used a dynamic resumable code fallback
    /// for scenarios where the F# compiler cannot generate static resumable code (e.g., in F# Interactive / FSI).
    /// With runtime-async intrinsics, this behaves the same as <see cref="taskSeq" />.
    /// </summary>
    val taskSeqDynamic: TaskSeqDynamicBuilder
