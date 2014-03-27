GpuLinq
=============
GpuLinq's main mission is to democratize GPGPU programming through LINQ. The main idea is that we represent the query as an Expression tree and after various transformations-optimizations we compile it into fast OpenCL kernel code. In addition we provide a very easy to work API without the need of messing with the details of the OpenCL API.


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
