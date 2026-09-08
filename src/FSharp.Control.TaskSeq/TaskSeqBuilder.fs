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
open System.Runtime.ExceptionServices

// the proper type from 0.4.0 onwards, see FSI file
type TaskSeq<'T> = IAsyncEnumerable<'T>

type EnumeratorState<'T> =
    | Completed
    | Canceled of OperationCanceledException
    | Disposed
    | Item of 'T
    | Faulted of ExceptionDispatchInfo

/// A one-shot awaitable signal used for the producer/consumer handshake,
/// based on ManualResetValueTaskSourceCore. GetResult rethrows the original
/// exception (unwrapped) via ExceptionDispatchInfo, matching resumable-code behavior.
/// For use by this library only, should not be used directly in user code.
[<NoComparison; NoEquality>]
type TaskSeqSignal<'T>() =
    // NOTE: RunContinuationsAsynchronously = false (the default) is deliberate and matches the
    // resumable-code implementation: the producer is driven inline by the consumer's MoveNextAsync
    // (and vice versa), so a synchronous producer runs to its next publish point synchronously.
    let source = ManualResetValueTaskSourceCore<'T>()

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
    ItemResponse: TaskSeqSignal<EnumeratorState<'T>>
    CancellationToken: CancellationToken
    KickOff: bool
}

// NOTE: these helpers are deliberately NOT inline and are used from inline builder
// member bodies: publishing through them keeps the inline fragments in a shape the
// runtime-async compiler analysis accepts.
module TaskSeqState =

    let publishResponse state value =
        state.ItemResponse.SetResult value

    let inline awaitNextMove state =
        state.MoveNextRequest.WaitAsync() |> AsyncHelpers.Await
        state.MoveNextRequest.Reset()
        state.CancellationToken.ThrowIfCancellationRequested()

/// The body of a taskSeq computation: a function that runs the computation
/// against the shared producer/consumer state. Values of this type only occur
/// inline, inside a runtime-async producer method. For use by this library only.
type TaskSeqCode<'T> = TaskSeqState<'T> -> unit

[<NoComparison; NoEquality>]
type internal TaskSeqEnumerator<'T>(startPRoducer, cancellationToken: CancellationToken) =
    let linkedCancellationSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
    let mutable current = Item Unchecked.defaultof<'T>

    let state = {
           MoveNextRequest = TaskSeqSignal<unit>()
           ItemResponse = TaskSeqSignal<_>()
           CancellationToken = linkedCancellationSource.Token
           KickOff = true
       }

    let producerTask = startPRoducer state

    interface IAsyncEnumerator<'T> with
        member _.Current =
            match current with
            | Item item -> item
            | _ ->
                // Returning a default value is similar to how F#'s seq<'T> behaves.
                // According to the docs, behavior of Current is Unspecified in this case.
                Unchecked.defaultof<'T>

        member this.MoveNextAsync() =
            __runtimeAsyncReturnValueTask (
                match current with
                | Completed | Canceled _ | Disposed -> false
                | Faulted edi -> edi.Throw(); false
                | _ ->
                state.ItemResponse.Reset()

                state.MoveNextRequest.SetResult()
                let next = state.ItemResponse.WaitAsync() |> AsyncHelpers.Await
                current <- next
                match next with
                | Faulted edi -> edi.Throw(); false
                | Canceled oce -> raise oce
                | Item item -> true
                | Disposed | Completed -> false
            )

        /// Disposes of the IAsyncEnumerator (*not* the IAsyncEnumerable!). Resumes the producer
        /// with a disposal request, so that pending `use` and `try/finally` compensations run,
        /// and waits for the producer to finish unwinding.
        member _.DisposeAsync() =
            __runtimeAsyncReturnValueTaskUnit (
                use cts = linkedCancellationSource

                match current with
                | Item _ ->
                    cts.Cancel false
                    // wake up the producer in case it is waiting for the next MoveNextAsync
                    state.MoveNextRequest.SetResult()
                    let response = state.ItemResponse.WaitAsync() |> AsyncHelpers.Await
                    current <- response
                    match response with
                    | Faulted edi -> edi.Throw()
                    | _ -> current <- Disposed
                | _ -> current <- Disposed
            )

[<Struct; NoComparison; NoEquality>]
type TaskSeqEnumerable<'T>(runProducer: TaskSeqState<'T> -> Task<unit>) =
    member _.RunProducer(state: TaskSeqState<'T>) = runProducer state

    interface IAsyncEnumerable<'T> with
        member this.GetAsyncEnumerator(cancellationToken) =
            TaskSeqEnumerator<'T>(runProducer, cancellationToken)

type TaskSeqBuilder() =

    member inline _.Delay([<InlineIfLambda>] generator: unit -> TaskSeqCode<'T>) : TaskSeqCode<'T> = fun state -> generator () state

    member inline _.Run([<InlineIfLambda>] code: TaskSeqCode<'T>) : IAsyncEnumerable<'T> =
        let runProducer (state: TaskSeqState<'T>) =
            __runtimeAsyncReturn (
                try 
                    // Wait for the consumer to kick off the enumeration.
                    if state.KickOff then
                        TaskSeqState.awaitNextMove state

                    code state
                    TaskSeqState.publishResponse state (Completed)
                with
                | :? OperationCanceledException as oce -> TaskSeqState.publishResponse state (Canceled oce)
                | error -> TaskSeqState.publishResponse state (Faulted (ExceptionDispatchInfo.Capture error))
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
            TaskSeqState.publishResponse state (Item value)
            TaskSeqState.awaitNextMove state

    member inline _.YieldFrom(source: seq<'T>) : TaskSeqCode<'T> =
        fun state ->
            for value in source do
                TaskSeqState.publishResponse state (Item value)
                TaskSeqState.awaitNextMove state

    member inline _.YieldFrom(source: #IAsyncEnumerable<'T>) : TaskSeqCode<'T> =
        fun state ->
            let innerEnumerator = source.GetAsyncEnumerator(state.CancellationToken)

            try
                while AsyncHelpers.Await(innerEnumerator.MoveNextAsync()) do
                    TaskSeqState.publishResponse state (Item innerEnumerator.Current)
                    TaskSeqState.awaitNextMove state
            finally
                AsyncHelpers.Await(innerEnumerator.DisposeAsync())

    //member inline this.YieldFromFinal(source : seq<'T>) : TaskSeqCode<'T> = this.YieldFrom source

    //member inline this.YieldFromFinal(source: IAsyncEnumerable<'T>) : TaskSeqCode<'T> =
    //    match source with
    //    | :? TaskSeqEnumerable<'T> as ts ->
    //        fun state ->
    //            AsyncHelpers.Await (ts.RunProducer { state with KickOff = false })
    //    | _ ->
    //        this.YieldFrom source

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
