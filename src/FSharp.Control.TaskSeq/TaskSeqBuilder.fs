namespace FSharp.Control

// note: runtime-async intrinsics are a preview compiler feature, requires '--langversion:preview'.
#nowarn "57" // Experimental library feature, requires '--langversion:preview'.
#nowarn "1204" // This construct is for use by compiled F# code and should not be used directly.

open System
open System.Collections.Generic
open System.Runtime.CompilerServices
open System.Threading
open System.Threading.Tasks
open System.Threading.Tasks.Sources

open FSharp.Core.CompilerServices
open FSharp.Core.CompilerServices.StateMachineHelpers
open FSharp.Control


[<AutoOpen>]
module Internal = // cannot be marked with 'internal' scope

    let initVerbose () =
        try
            match Environment.GetEnvironmentVariable "TASKSEQ_LOG_VERBOSE" with
            | null -> false
            | x ->
                match x.ToLowerInvariant().Trim() with
                | "1"
                | "true"
                | "on"
                | "yes" -> true
                | _ -> false

        with _ ->
            false

// deprecated from 0.4.0, see FSI file
[<Obsolete "From version 0.4.0 onward, 'TaskSeq<_>' is deprecated in favor of 'TaskSeq<_>'. It will be removed in an upcoming release.">]
type taskSeq<'T> = IAsyncEnumerable<'T>

// the proper type from 0.4.0 onwards, see FSI file
type TaskSeq<'T> = IAsyncEnumerable<'T>


/// Marker exception used to unwind the producer when the enumerator is disposed
/// before the sequence completed. It is caught by the producer wrapper and never
/// escapes the library. For use by this library only, should not be used directly in user code.
[<NoComparison; NoEquality>]
type TaskSeqDisposal() =
    inherit Exception()

/// A one-shot awaitable signal used for the producer/consumer handshake,
/// based on ManualResetValueTaskSourceCore. GetResult rethrows the original
/// exception (unwrapped) via ExceptionDispatchInfo, matching resumable-code behavior.
/// For use by this library only, should not be used directly in user code.
[<NoComparison; NoEquality>]
type TaskSeqSignal<'T>() =
    // NOTE: RunContinuationsAsynchronously = false (the default) is deliberate and matches the
    // resumable-code implementation: the producer is driven inline by the consumer's MoveNextAsync
    // (and vice versa), so a synchronous producer runs to its next publish point synchronously.
    let mutable source = ManualResetValueTaskSourceCore<'T>()

    member this.WaitAsync() = ValueTask<'T>(this, source.Version)
    member _.SetResult value = source.SetResult value
    member _.SetException(error: exn) = source.SetException error
    member _.Reset() = source.Reset()
    member _.Version = source.Version

    interface IValueTaskSource<'T> with
        member _.GetResult(token) = source.GetResult(token)
        member _.GetStatus(token) = source.GetStatus(token)

        member _.OnCompleted(continuation, continuationState, token, flags) =
            source.OnCompleted(continuation, continuationState, token, flags)

/// State shared between the producer (the taskSeq computation) and the consumer
/// (the IAsyncEnumerator) of a task sequence. For use by this library only.
[<NoComparison; NoEquality>]
type TaskSeqState<'T> = {
    /// Consumer -> Producer: set by MoveNextAsync to request the next item.
    MoveNextRequest: TaskSeqSignal<unit>
    /// Producer -> Consumer: completed with true (item available in Current), false (end of
    /// sequence), or an exception. This is the 'promiseOfValueOrEnd' of the resumable design.
    ItemResponse: TaskSeqSignal<bool>
    CancellationToken: CancellationToken
    /// Used by the IAsyncEnumerator interface to return the Current value.
    mutable Current: ValueOption<'T>
    /// Set by DisposeAsync to unwind the producer, running pending compensations.
    mutable DisposalRequested: bool
}

// NOTE: these helpers are deliberately NOT inline and are used from inline builder
// member bodies: publishing through them keeps the inline fragments in a shape the
// runtime-async compiler analysis accepts.
module TaskSeqState =
    let create cancellationToken = {
        MoveNextRequest = TaskSeqSignal<unit>()
        ItemResponse = TaskSeqSignal<bool>()
        CancellationToken = cancellationToken
        Current = ValueNone
        DisposalRequested = false
    }

    let publishItem state item =
        state.Current <- ValueSome item
        state.ItemResponse.SetResult true

    let publishCompleted state = state.ItemResponse.SetResult false
    let publishFaulted state (error: exn) = state.ItemResponse.SetException error
    let raiseDisposalRequested () = raise (TaskSeqDisposal())

    /// Reset the request signal after the consumer asked for the next item,
    /// and raise the disposal sentinel if the enumerator was disposed in the meantime.
    let resetAfterMoveNextRequest state =
        state.MoveNextRequest.Reset()

        if state.DisposalRequested then
            raiseDisposalRequested ()

/// The body of a taskSeq computation: a function that runs the computation
/// against the shared producer/consumer state. Values of this type only occur
/// inline, inside a runtime-async producer method. For use by this library only.
type TaskSeqCode<'T> = TaskSeqState<'T> -> unit

[<NoComparison; NoEquality>]
type internal TaskSeqEnumerator<'T>(state: TaskSeqState<'T>, producerTask: Task<unit>) =
    let mutable moveNextInProgress = 0
    let mutable completed = false
    let mutable disposed = false
    let mutable disposalSignaled = false

    interface IValueTaskSource<bool> with
        member _.GetResult(token) =
            Interlocked.Exchange(&moveNextInProgress, 0) |> ignore

            try
                match (state.ItemResponse :> IValueTaskSource<bool>).GetResult(token) with
                | true -> true
                | false ->
                    // Signal we reached the end (also for empty sequences).
                    completed <- true
                    state.Current <- ValueNone
                    false
            with _ ->
                // the producer faulted: rethrow the original exception, unwrapped
                completed <- true
                state.Current <- ValueNone
                reraise ()

        member _.GetStatus(token) = (state.ItemResponse :> IValueTaskSource<bool>).GetStatus(token)

        member _.OnCompleted(continuation, continuationState, token, flags) =
            (state.ItemResponse :> IValueTaskSource<bool>).OnCompleted(continuation, continuationState, token, flags)

    interface IAsyncEnumerator<'T> with
        member _.Current =
            match state.Current with
            | ValueSome x -> x
            | ValueNone ->
                // Returning a default value is similar to how F#'s seq<'T> behaves.
                // According to the docs, behavior of Current is Unspecified in this case.
                Unchecked.defaultof<'T>

        member this.MoveNextAsync() =
            if completed || disposed then
                // return False when beyond the last item, or after disposal
                ValueTask.False
            elif Interlocked.Exchange(&moveNextInProgress, 1) = 1 then
                invalidOp "MoveNextAsync cannot be called concurrently."
            else
                // Honor the cancellation token passed to GetAsyncEnumerator (fixes #179).
                // ThrowIfCancellationRequested() is a no-op for CancellationToken.None.
                state.CancellationToken.ThrowIfCancellationRequested()

                state.ItemResponse.Reset()
                // Request the next item from the producer, then hand out a ValueTask
                // that completes when the producer publishes it.
                state.MoveNextRequest.SetResult()
                ValueTask<bool>(this, state.ItemResponse.Version)

        /// Disposes of the IAsyncEnumerator (*not* the IAsyncEnumerable!). Resumes the producer
        /// with a disposal request, so that pending `use` and `try/finally` compensations run,
        /// and waits for the producer to finish unwinding.
        member _.DisposeAsync() =
            disposed <- true
            state.DisposalRequested <- true

            if not disposalSignaled then
                disposalSignaled <- true
                // wake up the producer in case it is waiting for the next MoveNextAsync
                state.MoveNextRequest.SetResult()

            __runtimeAsyncReturnValueTaskUnit (
                try
                    // the producer task never faults (Run publishes Faulted through the channel instead)
                    AsyncHelpers.Await producerTask
                with _ ->
                    ()
            )

[<Struct; NoComparison; NoEquality>]
type TaskSeqEnumerable<'T>(runProducer: TaskSeqState<'T> -> Task<unit>) =
    member _.RunProducer(state: TaskSeqState<'T>) = runProducer state

    interface IAsyncEnumerable<'T> with
        member this.GetAsyncEnumerator(cancellationToken) =
            let state = TaskSeqState.create cancellationToken
            let producerTask = runProducer state
            TaskSeqEnumerator<'T>(state, producerTask)

type TaskSeqBuilder() =

    member inline _.Delay([<InlineIfLambda>] generator: unit -> TaskSeqCode<'T>) : TaskSeqCode<'T> = fun state -> generator () state

    member inline _.Run([<InlineIfLambda>] code: TaskSeqCode<'T>) : IAsyncEnumerable<'T> =
        let runProducer (state: TaskSeqState<'T>) =
            __runtimeAsyncReturn (
                // Wait for the consumer to kick off the enumeration.
                AsyncHelpers.Await(state.MoveNextRequest.WaitAsync())
                state.MoveNextRequest.Reset()

                try
                    if state.DisposalRequested then
                        TaskSeqState.raiseDisposalRequested ()

                    code state
                    TaskSeqState.publishCompleted state
                with
                | :? TaskSeqDisposal -> ()
                | error -> TaskSeqState.publishFaulted state error
            )

        TaskSeqEnumerable(runProducer) :> IAsyncEnumerable<'T>

    member inline _.Zero() : TaskSeqCode<'T> = fun _ -> ()

    member inline _.ReturnFrom(task: Task) : TaskSeqCode<'T> = fun _ -> AsyncHelpers.Await task

    member inline _.ReturnFrom(task: Task<'U>) : TaskSeqCode<'T> = fun _ -> AsyncHelpers.Await task |> ignore

    member inline _.ReturnFrom(task: ValueTask) : TaskSeqCode<'T> = fun _ -> AsyncHelpers.Await task

    member inline _.ReturnFrom(task: ValueTask<'U>) : TaskSeqCode<'T> = fun _ -> AsyncHelpers.Await task |> ignore

    member inline _.ReturnFrom(computation: Async<'U>) : TaskSeqCode<'T> =
        fun _ ->
            AsyncHelpers.Await(Async.StartImmediateAsTask computation)
            |> ignore

    // NOTE: no concrete Bind for non-generic Task/ValueTask: having them alongside the
    // generic ones broke type inference for `use!` (and other unit-continuations).
    // They are handled by the SRTP overload in TaskSeqAwaitableExtensions below.

    member inline _.Bind(task: Task<'U>, [<InlineIfLambda>] continuation: 'U -> TaskSeqCode<'T>) : TaskSeqCode<'T> =
        fun state -> continuation (AsyncHelpers.Await task) state

    member inline _.Bind(task: ValueTask<'U>, [<InlineIfLambda>] continuation: 'U -> TaskSeqCode<'T>) : TaskSeqCode<'T> =
        fun state -> continuation (AsyncHelpers.Await task) state

    member inline _.Bind(computation: Async<'U>, [<InlineIfLambda>] continuation: 'U -> TaskSeqCode<'T>) : TaskSeqCode<'T> =
        fun state ->
            continuation (AsyncHelpers.Await(Async.StartImmediateAsTask(computation, cancellationToken = state.CancellationToken))) state

    member inline _.Combine([<InlineIfLambda>] task1: TaskSeqCode<'T>, [<InlineIfLambda>] task2: TaskSeqCode<'T>) : TaskSeqCode<'T> =
        fun state ->
            task1 state
            task2 state

    member inline _.TryWith([<InlineIfLambda>] body: TaskSeqCode<'T>, [<InlineIfLambda>] catch: exn -> TaskSeqCode<'T>) : TaskSeqCode<'T> =
        fun state ->
            try
                body state
            with error ->
                catch error state

    member inline _.TryFinally
        ([<InlineIfLambda>] body: TaskSeqCode<'T>, [<InlineIfLambda>] compensationAction: unit -> unit)
        : TaskSeqCode<'T> =
        fun state ->
            try
                body state
            finally
                compensationAction ()

    member inline _.TryFinallyAsync
        ([<InlineIfLambda>] body: TaskSeqCode<'T>, [<InlineIfLambda>] compensationAction: unit -> Task)
        : TaskSeqCode<'T> =
        fun state ->
            try
                body state
            finally
                AsyncHelpers.Await(compensationAction ())

    member inline _.Using(resource: 'Resource, [<InlineIfLambda>] body: 'Resource -> TaskSeqCode<'T>) : TaskSeqCode<'T> =
        fun state ->
            try
                body resource state
            finally
                match box resource with
                | :? IAsyncDisposable as disposable -> AsyncHelpers.Await(disposable.DisposeAsync())
                | :? IDisposable as disposable -> disposable.Dispose()
                | _ -> ()

    member inline _.While([<InlineIfLambda>] condition: unit -> bool, [<InlineIfLambda>] body: TaskSeqCode<'T>) : TaskSeqCode<'T> =
        fun state ->
            while condition () do
                body state

    /// Used by `For`. Unclear if `while!` (from F# 8.0) hits this.
    member inline _.WhileAsync
        ([<InlineIfLambda>] condition: unit -> ValueTask<bool>, [<InlineIfLambda>] body: TaskSeqCode<'T>)
        : TaskSeqCode<'T> =
        fun state ->
            let mutable conditionRes = true

            while conditionRes do
                conditionRes <- AsyncHelpers.Await(condition ())

                if conditionRes then
                    body state

    member inline _.For(sequence: seq<'U>, [<InlineIfLambda>] body: 'U -> TaskSeqCode<'T>) : TaskSeqCode<'T> =
        fun state ->
            for item in sequence do
                body item state

    member inline _.For(source: #IAsyncEnumerable<'U>, [<InlineIfLambda>] body: 'U -> TaskSeqCode<'T>) : TaskSeqCode<'T> =
        fun state ->
            let innerEnumerator = source.GetAsyncEnumerator(state.CancellationToken)

            try
                while AsyncHelpers.Await(innerEnumerator.MoveNextAsync()) do
                    body innerEnumerator.Current state
            finally
                AsyncHelpers.Await(innerEnumerator.DisposeAsync())

    member inline _.Yield(value: 'T) : TaskSeqCode<'T> =
        fun state ->
            // Publish the item, then wait until the consumer requests the next one.
            TaskSeqState.publishItem state value
            AsyncHelpers.Await(state.MoveNextRequest.WaitAsync())
            TaskSeqState.resetAfterMoveNextRequest state

    member inline _.YieldFrom(source: seq<'T>) : TaskSeqCode<'T> =
        fun state ->
            for value in source do
                TaskSeqState.publishItem state value
                AsyncHelpers.Await(state.MoveNextRequest.WaitAsync())
                TaskSeqState.resetAfterMoveNextRequest state

    member inline _.YieldFrom(source: #IAsyncEnumerable<'T>) : TaskSeqCode<'T> =
        fun state ->
            let innerEnumerator = source.GetAsyncEnumerator(state.CancellationToken)

            try
                while AsyncHelpers.Await(innerEnumerator.MoveNextAsync()) do
                    TaskSeqState.publishItem state innerEnumerator.Current
                    AsyncHelpers.Await(state.MoveNextRequest.WaitAsync())
                    TaskSeqState.resetAfterMoveNextRequest state
            finally
                AsyncHelpers.Await(innerEnumerator.DisposeAsync())

[<AutoOpen>]
module TaskSeqAwaitableExtensions =

    type TaskSeqBuilder with
        [<NoEagerConstraintApplication>]
        member inline _.Bind< ^TaskLike, 'T, 'U, ^Awaiter
            when ^TaskLike: (member GetAwaiter: unit -> ^Awaiter)
            and ^Awaiter :> ICriticalNotifyCompletion
            and ^Awaiter: (member get_IsCompleted: unit -> bool)
            and ^Awaiter: (member GetResult: unit -> 'T)>
            (task: ^TaskLike, [<InlineIfLambda>] continuation: 'T -> TaskSeqCode<'U>)
            : TaskSeqCode<'U> =
            fun state ->
                let awaiter = (^TaskLike: (member GetAwaiter: unit -> ^Awaiter) task)
                AsyncHelpers.AwaitAwaiter awaiter
                let result = (^Awaiter: (member GetResult: unit -> 'T) awaiter)
                continuation result state

open TaskSeqAwaitableExtensions

[<AutoOpen>]
module TaskSeqBuilder =
    let taskSeq = TaskSeqBuilder()

type TaskSeqDynamicBuilder() =
    inherit TaskSeqBuilder()

[<AutoOpen>]
module TaskSeqDynamicBuilder =
    let taskSeqDynamic = TaskSeqDynamicBuilder()
