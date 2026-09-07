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


/// The result of a single step of the producer side of a task sequence,
/// communicated to the consumer through a rendezvous handshake.
[<NoComparison; NoEquality>]
type TaskSeqEvent<'T> =
    | Item of 'T
    | Completed
    | Faulted of exn

/// Marker exception used to unwind the producer when the enumerator is disposed
/// before the sequence completed. It is caught by the producer wrapper and never
/// escapes the library. For use by this library only, should not be used directly in user code.
[<NoComparison; NoEquality>]
type TaskSeqDisposal() =
    inherit Exception()

[<NoComparison; NoEquality>]
type TaskSeqSignal<'T>() =
    let mutable source = ManualResetValueTaskSourceCore<'T>()
    do source.RunContinuationsAsynchronously <- true

    member this.WaitAsync() = ValueTask<'T>(this, source.Version)
    member _.SetResult value = source.SetResult value
    member _.Reset() = source.Reset()

    interface IValueTaskSource<'T> with
        member _.GetResult(token) = source.GetResult(token)
        member _.GetStatus(token) = source.GetStatus(token)

        member _.OnCompleted(continuation, state, token, flags) =
            source.OnCompleted(continuation, state, token, flags)

[<NoComparison; NoEquality>]
type TaskSeqState<'T> = {
    MoveNextRequest: TaskSeqSignal<unit>
    ItemResponse: TaskSeqSignal<TaskSeqEvent<'T>>
    CancellationToken: CancellationToken
    mutable DisposalRequested: bool
}

// NOTE: these helpers are deliberately NOT inline and are used from inline builder
// member bodies: publishing through them keeps the inline fragments in a shape the
// runtime-async compiler analysis accepts.
module TaskSeqState =
    let create cancellationToken = {
        MoveNextRequest = TaskSeqSignal<unit>()
        ItemResponse = TaskSeqSignal<TaskSeqEvent<'T>>()
        CancellationToken = cancellationToken
        DisposalRequested = false
    }

    let publishItem state item = state.ItemResponse.SetResult(Item item)
    let publishCompleted state = state.ItemResponse.SetResult(Completed)
    let publishFaulted state (error: exn) = state.ItemResponse.SetResult(Faulted error)
    let raiseDisposalRequested () = raise (TaskSeqDisposal ())

    /// Reset the request signal after the consumer asked for the next item,
    /// and raise the disposal sentinel if the enumerator was disposed in the meantime.
    let resetAfterMoveNextRequest state =
        state.MoveNextRequest.Reset()

        if state.DisposalRequested then
            raiseDisposalRequested ()

type TaskSeqCode<'T> = TaskSeqState<'T> -> unit

[<NoComparison; NoEquality>]
type internal TaskSeqEnumerator<'T>(state: TaskSeqState<'T>, producerTask: Task<unit>) =
    let mutable current = ValueNone
    let mutable moveNextInProgress = 0
    let mutable completed = false
    let mutable disposed = false
    let mutable disposalSignaled = false

    interface IAsyncEnumerator<'T> with
        member _.Current =
            match current with
            | ValueSome x -> x
            | ValueNone -> Unchecked.defaultof<'T>

        member _.MoveNextAsync() =
            if completed || disposed then
                // return False when beyond the last item, or after disposal
                ValueTask.False
            elif Interlocked.Exchange(&moveNextInProgress, 1) = 1 then
                invalidOp "MoveNextAsync cannot be called concurrently."
            else
                state.CancellationToken.ThrowIfCancellationRequested()

                __runtimeAsyncReturnValueTask<bool> (
                    try
                        state.MoveNextRequest.SetResult()

                        match AsyncHelpers.Await(state.ItemResponse.WaitAsync()) with
                        | Item value ->
                            current <- ValueSome value
                            true
                        | Completed ->
                            current <- ValueNone
                            completed <- true
                            false
                        | Faulted error ->
                            current <- ValueNone
                            completed <- true
                            raise error
                    finally
                        state.ItemResponse.Reset()
                        Interlocked.Exchange(&moveNextInProgress, 0) |> ignore
                )

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

    member inline _.Delay([<InlineIfLambda>] generator: unit -> TaskSeqCode<'T>) : TaskSeqCode<'T> =
        fun state -> generator () state

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
                | error ->
                    TaskSeqState.publishFaulted state error
            )

        TaskSeqEnumerable(runProducer) :> IAsyncEnumerable<'T>

    member inline _.Zero() : TaskSeqCode<'T> = fun _ -> ()

    member inline _.ReturnFrom(task: Task) : TaskSeqCode<'T> =
        fun _ -> AsyncHelpers.Await task

    member inline _.ReturnFrom(task: Task<'U>) : TaskSeqCode<'T> =
        fun _ -> AsyncHelpers.Await task |> ignore

    member inline _.ReturnFrom(task: ValueTask) : TaskSeqCode<'T> =
        fun _ -> AsyncHelpers.Await task

    member inline _.ReturnFrom(task: ValueTask<'U>) : TaskSeqCode<'T> =
        fun _ -> AsyncHelpers.Await task |> ignore

    member inline _.ReturnFrom(computation: Async<'U>) : TaskSeqCode<'T> =
        fun _ -> AsyncHelpers.Await(Async.StartImmediateAsTask computation) |> ignore

    // NOTE: no concrete Bind for non-generic Task/ValueTask: having them alongside the
    // generic ones broke type inference for `use!` (and other unit-continuations).
    // They are handled by the SRTP overload in TaskSeqAwaitableExtensions below.

    member inline _.Bind(task: Task<'U>, [<InlineIfLambda>] continuation: 'U -> TaskSeqCode<'T>) : TaskSeqCode<'T> =
        fun state -> continuation (AsyncHelpers.Await task) state

    member inline _.Bind(task: ValueTask<'U>, [<InlineIfLambda>] continuation: 'U -> TaskSeqCode<'T>) : TaskSeqCode<'T> =
        fun state -> continuation (AsyncHelpers.Await task) state

    member inline _.Bind(computation: Async<'U>, [<InlineIfLambda>] continuation: 'U -> TaskSeqCode<'T>) : TaskSeqCode<'T> =
        fun state ->
            continuation
                (AsyncHelpers.Await(
                    Async.StartImmediateAsTask(computation, cancellationToken = state.CancellationToken)
                ))
                state

    member inline _.Combine(task1: TaskSeqCode<'T>, [<InlineIfLambda>] task2: TaskSeqCode<'T>) : TaskSeqCode<'T> =
        fun state ->
            task1 state
            task2 state

    member inline _.TryWith([<InlineIfLambda>] body: TaskSeqCode<'T>, [<InlineIfLambda>] catch: exn -> TaskSeqCode<'T>) : TaskSeqCode<'T> =
        fun state ->
            try
                body state
            with error ->
                catch error state

    member inline _.TryFinally([<InlineIfLambda>] body: TaskSeqCode<'T>, [<InlineIfLambda>] compensationAction: unit -> unit) : TaskSeqCode<'T> =
        fun state ->
            try
                body state
            finally
                compensationAction ()

    member inline _.TryFinallyAsync([<InlineIfLambda>] body: TaskSeqCode<'T>, [<InlineIfLambda>] compensationAction: unit -> Task) : TaskSeqCode<'T> =
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
    member inline _.WhileAsync([<InlineIfLambda>] condition: unit -> ValueTask<bool>, [<InlineIfLambda>] body: TaskSeqCode<'T>) : TaskSeqCode<'T> =
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


