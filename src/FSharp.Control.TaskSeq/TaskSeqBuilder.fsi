namespace FSharp.Control

open System
open System.Runtime.CompilerServices
open System.Threading.Tasks
open System.Collections.Generic
open FSharp.Core.CompilerServices

/// <summary>
/// Represents a task sequence and is the output of using the <paramref name="taskSeq{...}" />
/// computation expression from this library. It is an alias for <see cref="T:System.IAsyncEnumerable&lt;_>" />.
/// </summary>
type TaskSeq<'T> = IAsyncEnumerable<'T>

/// <summary>
/// Main builder class for the <see cref="taskSeq" /> computation expression.
/// </summary>
type TaskSeqBuilder =

    new: unit -> TaskSeqBuilder
    member inline Zero: unit -> seq<'T>
    member inline Yield: value: 'T -> seq<'T>
    member inline Delay: body: (unit -> seq<'T>) -> (unit -> seq<'T>)
    member inline Combine: first: seq<'T> * rest: (unit -> seq<'T>) -> seq<'T>
    member inline For: source: seq<'U> * body: ('U -> seq<'T>) -> seq<'T>
    member inline For: source: IAsyncEnumerable<'U> * body: ('U -> seq<'T>) -> seq<'T>
    member inline While: guard: (unit -> bool) * body: (unit -> seq<'T>) -> seq<'T>
    /// Used by `For`. Unclear if `while!` (from F# 8.0) hits this
    member inline WhileAsync: condition: (unit -> ValueTask<bool>) * body: (unit -> seq<'T>) -> seq<'T>
    member inline TryFinally: body: (unit -> seq<'T>) * compensation: (unit -> unit) -> seq<'T>
    member inline TryFinallyAsync: body: (unit -> seq<'T>) * compensation: (unit -> Task) -> seq<'T>
    member inline TryWith: body: (unit -> seq<'T>) * handler: (exn -> seq<'T>) -> seq<'T>
    member inline Using: resource: 'R * body: ('R -> seq<'T>) -> seq<'T>
    member inline Bind: source: Task<'U> * continuation: ('U -> seq<'T>) -> seq<'T>
    member inline Bind: source: ValueTask<'U> * continuation: ('U -> seq<'T>) -> seq<'T>
    member inline Bind: source: Async<'U> * continuation: ('U -> seq<'T>) -> seq<'T>
    member inline Run: recipe: (unit -> seq<'T>) -> IAsyncEnumerable<'T>
    member inline YieldFrom: source: seq<'T> -> seq<'T>
    member inline YieldFrom: source: IAsyncEnumerable<'T> -> seq<'T>

[<AutoOpen>]
module TaskSeqAwaitableExtensions =

    /// <summary>
    /// Contains extension methods for the main builder class for the <see cref="taskSeq" /> computation expression.
    /// This module is not meant to be accessed directly from user code.
    /// </summary>

    type TaskSeqBuilder with

        /// <summary>Lowest-priority SRTP fallback for binding custom awaitables (task-like types).</summary>
        [<NoEagerConstraintApplication>]
        member inline Bind< ^TaskLike, 'T, 'U, ^Awaiter> :
            task: ^TaskLike * continuation: ('T -> seq<'U>) -> seq<'U>
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

[<AutoOpen>]
module TaskSeqDynamicBuilder =
    /// <summary>
    /// Builds an asynchronous task sequence.
    /// </summary>
    val taskSeqDynamic: TaskSeqBuilder
