GpuLinq
=============
An automatic query optimizer-compiler for LINQ targeting GPUs. 
GpuLinq is based on hte LinqOptimizer project and compiles declarative LINQ queries into GPU kernels using the OpenCL.Net project.


```csharp
using (var context = new GpuContext())
{
    using (var vs = context.CreateGpuArray(vector))
    {
        // Dot Product
        var query = GpuQueryExpr.Zip(vs, vs, (a, b) => a * b).Sum();
        var result = context.Run(query);
    }
}
```

References
----------
* [LinqOptimizer](https://github.com/nessos/LinqOptimizer)
