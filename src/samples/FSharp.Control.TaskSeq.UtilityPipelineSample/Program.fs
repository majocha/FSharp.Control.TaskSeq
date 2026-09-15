module UtilityPipelineSample

open System
open System.Diagnostics
open FSharp.Control

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

let runOnce values =
    let sw = Stopwatch()
    sw.Start()
    let result = utilityPipeline values |> Async.AwaitTask |> Async.RunSynchronously
    sw.Stop()
    sw.ElapsedMilliseconds, result.Length

[<EntryPoint>]
let main argv =
    let size =
        match argv with
        | [| value |] when int value > 0 -> int value
        | _ -> 100_000

    let repeatCount =
        match argv with
        | [| _; repeats |] when int repeats > 0 -> int repeats
        | _ -> 5

    let values = Array.init size id
    let results = Array.init repeatCount (fun _ -> runOnce values)

    let totalMs = results |> Array.sumBy fst
    let averageMs = totalMs / int64 repeatCount
    let maxMs = results |> Array.maxBy fst |> fst
    let minMs = results |> Array.minBy fst |> fst
    let lastLength = snd results.[results.Length - 1]

    printfn "UtilityPipeline sample"
    printfn "size=%d repeats=%d" size repeatCount
    printfn "totalMs=%d averageMs=%d minMs=%d maxMs=%d lastLength=%d" totalMs averageMs minMs maxMs lastLength

    0
