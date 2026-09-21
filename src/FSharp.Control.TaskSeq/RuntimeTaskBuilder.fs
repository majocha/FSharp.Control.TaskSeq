namespace Microsoft.FSharp.Control

open System
open System.Runtime.CompilerServices
open System.Threading
open System.Threading.Tasks
open System.Collections.Generic
open Microsoft.FSharp.Core
open Microsoft.FSharp.Core.CompilerServices
open Microsoft.FSharp.Core.LanguagePrimitives.IntrinsicOperators
open Microsoft.FSharp.Collections

module TasklikeHelpers =

    /// A structure that looks like an Awaiter
    type Awaiter<'Awaiter, 'TResult
        when 'Awaiter :> ICriticalNotifyCompletion
        and 'Awaiter: (member get_IsCompleted: unit -> bool)
        and 'Awaiter: (member GetResult: unit -> 'TResult)> = 'Awaiter

    type Awaitable<'Awaitable, 'Awaiter, 'TResult
        when 'Awaitable: (member GetAwaiter: unit -> Awaiter<'Awaiter, 'TResult>)> = 'Awaitable

    module Awaiter =
        let inline isCompleted (awaiter: Awaiter<_, _>) = awaiter.get_IsCompleted ()
        let inline getResult (awaiter: Awaiter<_, _>) = awaiter.GetResult()
        let inline onCompleted (awaiter: Awaiter<_, _>) continuation = awaiter.OnCompleted continuation
        let inline unsafeOnCompleted (awaiter: Awaiter<_, _>) continuation = awaiter.UnsafeOnCompleted continuation

    module Awaitable =
        let inline getAwaiter (awaitable: Awaitable<_, _, _>) = awaitable.GetAwaiter()

open TasklikeHelpers

module RuntimeAsyncBuilderHelpers =

    [<RequireQualifiedAccess>]
    module Cancellation =
        // AsyncLocal is a natural fit to store the cancellation token and make it available across suspensions
        // without explicitly threading the state through the builder.
        let token = AsyncLocal<CancellationToken>()

        let inline setToken ct = token.Value <- ct

        let inline check() =
            token.Value.ThrowIfCancellationRequested()

    // A delegate to unify dissimilar builder source types, this allows us to have no additional Bind or MergeSources overloads.
    // The delegate's invocation is inlined, so this is zero cost.
    type Started<'T> = delegate of unit -> 'T

    [<NoEagerConstraintApplication>]
    let inline startAwaitable awaitable =
        // Make sure the delegate captures only started awaitables to make MergeSources concurrent.
        let awaiter = Awaitable.getAwaiter awaitable
        Started(fun () ->
            Cancellation.check()
            AsyncHelpers.UnsafeAwaitAwaiter awaiter
            Awaiter.getResult awaiter)

open RuntimeAsyncBuilderHelpers

module RuntimeAsyncBuilder =
    let inline isAlreadyBackground () =
        isNull SynchronizationContext.Current && obj.ReferenceEquals(TaskScheduler.Current, TaskScheduler.Default)

    // This will get inlined into the 
    let inline runImpl([<InlineIfLambda>] body: unit -> 'T) ct =
        Cancellation.setToken ct
        body()

    let inline runImplNoCancellation([<InlineIfLambda>] body: unit -> 'T) =
        Cancellation.setToken CancellationToken.None
        body()

type RuntimeAsyncBuilder() =

    member inline _.Delay([<InlineIfLambda>] generator: unit -> 'T) =
        fun () ->
            Cancellation.check ()
            generator()

    member inline _.Zero() = ()
    member inline _.Return(value: 'T) = value

    member inline _.Combine(first: unit, [<InlineIfLambda>] second) =
        ignore first
        second()

    member inline _.Combine(first, [<InlineIfLambda>] second) =
        first()
        second()
    member inline _.TryWith([<InlineIfLambda>] body: unit -> 'T, [<InlineIfLambda>] handler: exn -> 'T) =
        try body() with error -> handler error
    member inline _.TryFinally([<InlineIfLambda>] body: unit -> 'T, [<InlineIfLambda>] compensation: unit -> unit) =
        try body() finally compensation()
    member inline _.Using(resource, [<InlineIfLambda>] body) =
        try
            body resource
        finally
            match box resource with
            | :? IAsyncDisposable as disposable -> AsyncHelpers.Await(disposable.DisposeAsync())
            | :? IDisposable as disposable -> disposable.Dispose()
            | _ -> ()

    member inline _.While(guard: unit -> bool, [<InlineIfLambda>] body: unit -> unit) =
        while guard() do body()

    member inline _.For(sequence: seq<'T>, [<InlineIfLambda>] body: 'T -> unit) =
        for item in sequence do body item

    member inline this.For(sequence: IAsyncEnumerable<'T>, [<InlineIfLambda>] body: 'T -> unit) =
        this.Using(sequence.GetAsyncEnumerator(Cancellation.token.Value), fun enumerator ->
            while enumerator.MoveNextAsync() |> AsyncHelpers.Await do
                body enumerator.Current)

    member inline _.Bind([<InlineIfLambda>] awaited: Started<'T>, [<InlineIfLambda>] continuation) =
        awaited.Invoke() |> continuation

    member inline _.ReturnFrom([<InlineIfLambda>]  awaited: Started<'T>) = awaited.Invoke()

    member inline _.MergeSources([<InlineIfLambda>] left: Started<'A>, [<InlineIfLambda>] right: Started<'B>) =
        Started(fun () ->
            let left = left.Invoke()
            let right = right.Invoke()
            struct (left, right))

    // sources consumed by For method
    member inline _.Source(sequence: 'T seq) = sequence
    member inline _.Source(sequence: IAsyncEnumerable<'T>) = sequence

    // Cannonical runtime async sources 
    member inline _.Source(task: Task<'T>) = Started(fun () -> task |> AsyncHelpers.Await)
    member inline _.Source(task: Task) = Started(fun () -> task |> AsyncHelpers.Await)
    member inline _.Source(task: ValueTask<'T>) = Started(fun () -> task |> AsyncHelpers.Await)
    member inline _.Source(task: ValueTask) = Started(fun () -> task |> AsyncHelpers.Await)

    // Bind also cold-start async computations
    member inline _.Source(computation: Async<'T>) =
        let task = Async.StartImmediateAsTask(computation, Cancellation.token.Value)
        Started(fun () -> task |> AsyncHelpers.Await)

[<AutoOpen>]
module RuntimeTask =

    open RuntimeAsyncBuilder
    
    type RuntimeTaskBuilder() =
        inherit RuntimeAsyncBuilder()
        member inline _.Run([<InlineIfLambda>] code) : Task<'T> =
            __runtimeAsyncReturn( runImplNoCancellation code)

    let runtimeTask = RuntimeTaskBuilder()

    type BackgroundRuntimeTaskBuilder() =
        inherit RuntimeAsyncBuilder()
        member inline _.Run([<InlineIfLambda>] code: unit -> 'T) : Task<'T> =
            if isAlreadyBackground() then
                __runtimeAsyncReturn(runImplNoCancellation code)
            else
            Task.Run<'T>(fun () -> __runtimeAsyncReturn (runImplNoCancellation code))

    let backgroundRuntimeTask = BackgroundRuntimeTaskBuilder()

[<AutoOpen>]
module RuntimeAsyncBuilderAwaitableExtensions =
    type RuntimeAsyncBuilder with
        member inline _.Source(awaitable) = startAwaitable awaitable
