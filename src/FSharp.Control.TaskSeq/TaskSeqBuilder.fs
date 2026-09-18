namespace FSharp.Control

// note: runtime-async intrinsics are a preview compiler feature, requires '--langversion:preview'.
#nowarn "57" // Experimental library feature, requires '--langversion:preview'.
#nowarn "1204" // This construct is for use by compiled F# code and should not be used directly.

open System
open System.Collections.Generic
open System.Runtime.CompilerServices
open System.Threading.Tasks
open Microsoft.FSharp.Core.CompilerServices

// the proper type from 0.4.0 onwards, see FSI file
type TaskSeq<'T> = IAsyncEnumerable<'T>

type TaskSeqBuilder() =

    member inline _.Zero() = Seq.empty

    member inline _.Yield(value) = Seq.singleton value

    member inline _.Delay([<InlineIfLambda>] body: unit -> seq<'T>) = body

    member inline _.Combine(first, [<InlineIfLambda>] rest) =
        Seq.append first (Seq.delay (fun () -> rest ()))

    member inline _.For(source: seq<'U>, [<InlineIfLambda>] body: 'U -> seq<'T>) =
        Seq.collect (fun value -> body value) source

    member inline _.While([<InlineIfLambda>] guard, [<InlineIfLambda>] body) =
        RuntimeHelpers.EnumerateWhile (fun () -> guard ()) (Seq.delay (fun () -> body ()))

    member inline _.TryFinally([<InlineIfLambda>] body, [<InlineIfLambda>] compensation) =
        RuntimeHelpers.EnumerateThenFinally (Seq.delay (fun () -> body ())) (fun () -> compensation ())

    member inline _.TryFinallyAsync([<InlineIfLambda>] body, [<InlineIfLambda>] compensation: unit -> Task) =
        RuntimeHelpers.EnumerateThenFinally
            (Seq.delay (fun () -> body ()))
            (fun () -> AsyncHelpers.Await(compensation ()))

    member inline _.TryWith([<InlineIfLambda>] body, [<InlineIfLambda>] handler) =
        RuntimeHelpers.EnumerateTryWith (Seq.delay (fun () -> body ())) (fun _ -> 1) handler

    member inline _.Using(resource: 'R, [<InlineIfLambda>] body: 'R -> seq<'T>) =
        RuntimeHelpers.EnumerateThenFinally
            (Seq.delay (fun () -> body resource))
            (fun () ->
                match box resource with
                | :? IAsyncDisposable as disposable -> AsyncHelpers.Await(disposable.DisposeAsync())
                | :? IDisposable as disposable -> disposable.Dispose()
                | _ -> ())

    member inline this.For(source: IAsyncEnumerable<'U>, [<InlineIfLambda>] body: 'U -> seq<'T>) =
        Seq.delay (fun () ->
            let iterator = source.GetAsyncEnumerator()

            this.Using(
                iterator,
                fun iterator ->
                    this.While((fun () -> AsyncHelpers.Await(iterator.MoveNextAsync())), (fun () -> body iterator.Current))
            ))

    member inline this.YieldFrom(source: seq<'T>) =
        this.For(source, fun value -> this.Yield value)

    member inline this.YieldFrom(source: IAsyncEnumerable<'T>) =
        this.For(source, fun value -> this.Yield value)

    // NOTE: no concrete Bind for non-generic Task/ValueTask: having them alongside the
    // generic ones broke type inference for `use!` (and other unit-continuations).
    // They are handled by the SRTP overload in TaskSeqAwaitableExtensions below.
    member inline _.Bind(source: Task<'U>, [<InlineIfLambda>] continuation: 'U -> seq<'T>) =
        continuation (AsyncHelpers.Await source)

    member inline _.Bind(source: ValueTask<'U>, [<InlineIfLambda>] continuation: 'U -> seq<'T>) =
        continuation (AsyncHelpers.Await source)

    member inline _.Bind(source: Async<'U>, [<InlineIfLambda>] continuation: 'U -> seq<'T>) =
        continuation (AsyncHelpers.Await(Async.StartImmediateAsTask source))

    member inline this.WhileAsync([<InlineIfLambda>] condition: unit -> ValueTask<bool>, [<InlineIfLambda>] body) =
        this.While((fun () -> AsyncHelpers.Await(condition ())), body)

    member inline _.Run([<InlineIfLambda>] recipe: unit -> seq<'T>) : IAsyncEnumerable<'T> =
        StateMachineHelpers.__runtimeAsyncSequence recipe

[<AutoOpen>]
module TaskSeqAwaitableExtensions =

    type TaskSeqBuilder with
        [<NoEagerConstraintApplication>]
        member inline _.Bind< ^TaskLike, 'T, 'U, ^Awaiter
            when ^TaskLike: (member GetAwaiter: unit -> ^Awaiter)
            and ^Awaiter :> ICriticalNotifyCompletion
            and ^Awaiter: (member get_IsCompleted: unit -> bool)
            and ^Awaiter: (member GetResult: unit -> 'T)>
            (task: ^TaskLike, [<InlineIfLambda>] continuation: 'T -> seq<'U>)
            : seq<'U> =
            let awaiter = (^TaskLike: (member GetAwaiter: unit -> ^Awaiter) task)

            if not (^Awaiter: (member get_IsCompleted: unit -> bool) awaiter) then
                AsyncHelpers.UnsafeAwaitAwaiter awaiter

            continuation (^Awaiter: (member GetResult: unit -> 'T) awaiter)

open TaskSeqAwaitableExtensions

[<AutoOpen>]
module TaskSeqBuilder =
    let taskSeq = TaskSeqBuilder()

[<AutoOpen>]
module TaskSeqDynamicBuilder =
    let taskSeqDynamic = TaskSeqBuilder()
