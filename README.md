GpuLinq
=============
An automatic query optimizer-compiler for LINQ targeting GPUs. 
GpuLinq is based on the LinqOptimizer project and compiles declarative LINQ queries into GPU kernels using the OpenCL.Net project.


```csharp
using (var context = new GpuContext())
{
    using (var nums = context.CreateGpuArray(array))
    {
        
        var query = (from num in nums.AsGpuQueryExpr()
                     where num % 2 == 0
                     select num * num).Sum();
        var result = context.Run(query);
    }
}
```

References
----------
* [LinqOptimizer](https://github.com/nessos/LinqOptimizer)
