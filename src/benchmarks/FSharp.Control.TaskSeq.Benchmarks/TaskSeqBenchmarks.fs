namespace Benchmarks

open System.Threading
open System.Threading.Tasks
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Running
open FSharp.Control

module BenchmarksHelpers =
    let rangeSource count =
        taskSeq {
            for value in 1 .. count do
                yield value
        }

    let selfRecursiveSource count : TaskSeq<int> =
        let rec loop current : TaskSeq<int> =
            taskSeq {
                if current <= 0 then
                    ()
                else
                    yield current
                    yield! loop (current - 1)
            }

        loop count

    let mutualRecursiveSource count : TaskSeq<int> =
        let rec left current : TaskSeq<int> =
            taskSeq {
                if current <= 0 then
                    ()
                else
                    yield current
                    yield! right (current - 1)
            }

        and right current : TaskSeq<int> =
            taskSeq {
                if current <= 0 then
                    ()
                else
                    yield current * 10
                    yield! left (current - 1)
            }

        left count

    let multipleEnumerators count enumeratorCount =
        let source = rangeSource count

        task {
            let mutable total = 0

            for _ in 1 .. enumeratorCount do
                use e = source.GetAsyncEnumerator CancellationToken.None

                while! e.MoveNextAsync() do
                    total <- total + e.Current

            return total
        }

    let mappedFilteredSource count =
        rangeSource count
        |> TaskSeq.map (fun x -> x * 2)
        |> TaskSeq.filter (fun x -> x % 3 = 0)

    let asyncRecursiveSource count : TaskSeq<int> =
        let rec loop current : TaskSeq<int> =
            taskSeq {
                if current <= 0 then
                    ()
                else
                    do! Task.Delay(0)
                    yield current
                    yield! loop (current - 1)
            }

        loop count

[<MemoryDiagnoser>]
type TaskSeqBenchmarks() =
    [<Params(100, 1000)>]
    member val Count = 0 with get, set

    [<Benchmark(Baseline = true)>]
    member this.LinearRange() : Task<int[]> =
        BenchmarksHelpers.rangeSource this.Count |> TaskSeq.toArrayAsync

    [<Benchmark>]
    member this.SelfRecursive() : Task<int[]> =
        BenchmarksHelpers.selfRecursiveSource this.Count |> TaskSeq.toArrayAsync

    [<Benchmark>]
    member this.MutualRecursive() : Task<int[]> =
        BenchmarksHelpers.mutualRecursiveSource this.Count |> TaskSeq.toArrayAsync

    [<Benchmark>]
    member this.MultipleEnumerators() : Task<int> =
        BenchmarksHelpers.multipleEnumerators this.Count 4

    [<Benchmark>]
    member this.MappedAndFiltered() : Task<int[]> =
        BenchmarksHelpers.mappedFilteredSource this.Count |> TaskSeq.toArrayAsync

    [<Benchmark>]
    member this.AsyncRecursive() : Task<int[]> =
        BenchmarksHelpers.asyncRecursiveSource this.Count |> TaskSeq.toArrayAsync

module Program =
    [<EntryPoint>]
    let main args =
        BenchmarkRunner.Run<TaskSeqBenchmarks>() |> ignore
        0
