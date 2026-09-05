# TaskSeq benchmarks

The benchmark source is compiled into two executables so the repository build and released package do not share the same assembly identity in one process:

- `FSharp.Control.TaskSeq.Benchmarks.Local` references the repository project.
- `FSharp.Control.TaskSeq.Benchmarks.Released` references `FSharp.Control.TaskSeq` version `1.1.1`.

Build and run both projects in Release configuration from Visual Studio or with the corresponding project commands. BenchmarkDotNet writes reports under each executable's `BenchmarkDotNet.Artifacts` directory. Compare the matching `ToArrayAsync` results for the 100- and 10,000-element cases, including the allocation columns from `MemoryDiagnoser`.
