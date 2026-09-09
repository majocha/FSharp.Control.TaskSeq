namespace Benchmarks

open System
open System.Collections.Generic
open System.IO
open System.Threading.Tasks
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Running
open FSharp.Control

module BenchmarksHelpers =
    type AsyncBufferedReader(data: byte[], blockSize: int) =
        let stream = new MemoryStream(data, writable = false)
        let buffered = new BufferedStream(stream, blockSize)
        let mutable current = ValueNone

        interface IAsyncEnumerable<byte[]> with
            member reader.GetAsyncEnumerator _ = reader :> IAsyncEnumerator<byte[]>

        interface IAsyncEnumerator<byte[]> with
            member _.Current =
                match current with
                | ValueSome value -> value
                | ValueNone -> failwith "Not a current item"

            member _.MoveNextAsync() =
                task {
                    let buffer = Array.zeroCreate blockSize
                    let! bytesRead = buffered.ReadAsync(buffer, 0, buffer.Length)

                    if bytesRead > 0 then
                        current <- ValueSome buffer
                        return true
                    else
                        current <- ValueNone
                        return false
                }
                |> Task.toValueTask

            member _.DisposeAsync() =
                buffered.Dispose()
                ValueTask()

    let recursiveRange count : TaskSeq<int> =
        let rec loop current =
            taskSeq {
                if current <= count then
                    yield current
                    yield! loop (current + 1)
            }

        loop 1

    let mutuallyRecursiveRange count : TaskSeq<int> =
        let rec odd current : TaskSeq<int> =
            taskSeq {
                if current <= count then
                    yield current
                    yield! even (current + 1)
            }

        and even current : TaskSeq<int> =
            taskSeq {
                if current <= count then
                    yield current
                    yield! odd (current + 1)
            }

        odd 1

    let enumerateConcurrently enumeratorCount (source: TaskSeq<int>) =
        [| for _ in 1 .. enumeratorCount -> source |> TaskSeq.toArrayAsync |]
        |> Task.WhenAll
        |> Task.map (Array.sumBy Array.length)

    let utilityPipeline values =
        let first = values |> TaskSeq.ofArray
        let second = values |> Array.map ((*) 2) |> TaskSeq.ofArray

        first
        |> TaskSeq.append second
        |> TaskSeq.indexed
        |> TaskSeq.map (fun (index, value) -> value + index)
        |> TaskSeq.filter (fun value -> value % 3 <> 0)
        |> TaskSeq.choose (fun value ->
            if value % 5 = 0 then Some value else None)
        |> TaskSeq.collect (fun value -> TaskSeq.ofArray [| value; value + 1 |])
        |> TaskSeq.distinct
        |> TaskSeq.chunkBySize 64
        |> TaskSeq.collectSeq Array.toSeq
        |> TaskSeq.truncate 10_000
        |> TaskSeq.toArrayAsync

[<MemoryDiagnoser; ShortRunJob>]
type TaskSeqBenchmarks() =
    [<Params(10_000, 100_000)>]
    member val ArrayLength = 0 with get, set

    member val Values = [||] with get, set

    [<GlobalSetup>]
    member this.Setup() =
        this.Values <- Array.init this.ArrayLength id
        this.Data <- Array.zeroCreate 1_048_576

    [<Benchmark(Baseline = true)>]
    member this.EnumerateLargeArray() : Task<int[]> =
        this.Values |> TaskSeq.ofArray |> TaskSeq.toArrayAsync

    [<Benchmark>]
    member this.FilterLargeArray() : Task<int[]> =
        this.Values
        |> TaskSeq.ofArray
        |> TaskSeq.filter (fun value -> value % 16 = 0)
        |> TaskSeq.toArrayAsync

    [<Benchmark>]
    member this.EnumerateLargeArrayConcurrently() : Task<int> =
        this.Values
        |> TaskSeq.ofArray
        |> BenchmarksHelpers.enumerateConcurrently 2

    [<Benchmark>]
    member this.UtilityPipeline() : Task<int[]> =
        BenchmarksHelpers.utilityPipeline this.Values

    member val Data = [||] with get, set

    [<Benchmark>]
    member this.ConsumeMegabyteAsyncBufferedReader() : Task<byte[][]> =
        let reader = BenchmarksHelpers.AsyncBufferedReader(this.Data, 256)
        reader |> TaskSeq.toArrayAsync

    [<Benchmark>]
    member this.EnumerateRecursively() : Task<int[]> =
        BenchmarksHelpers.recursiveRange (min this.ArrayLength 500) |> TaskSeq.toArrayAsync

    [<Benchmark>]
    member this.EnumerateMutuallyRecursively() : Task<int[]> =
        BenchmarksHelpers.mutuallyRecursiveRange (min this.ArrayLength 500) |> TaskSeq.toArrayAsync

module Program =
    [<EntryPoint>]
    let main args =
        BenchmarkRunner.Run<TaskSeqBenchmarks>() |> ignore
        0
