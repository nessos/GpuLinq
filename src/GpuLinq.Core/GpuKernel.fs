namespace Nessos.GpuLinq.Core
    open System
    open System.Linq.Expressions;
    open OpenCL.Net.Extensions
    open OpenCL.Net

    open Nessos.LinqOptimizer.Core

    /// <summary>
    /// Interface for managing GPU Kernels
    /// </summary>
    type IGpuKernel =
        inherit IDisposable
        abstract member Kernel : Kernel
        abstract member Args : seq<ParameterExpression>
        abstract member Query : QueryExpr
        abstract member CompilerResult : Compiler.CompilerResult
        


    /// <summary>
    /// A base object for managing GPU Kernels
    /// </summary>
    type GpuKernel(kernel : Kernel, args : seq<ParameterExpression>, 
                                    query : QueryExpr,
                                    compilerResult : Compiler.CompilerResult) = 
        let mutable disposed = false
        interface IGpuKernel with 
                member self.Kernel = kernel
                member self.Args = args
                member self.Query = query
                member self.CompilerResult = compilerResult
                
        interface System.IDisposable with 
            member this.Dispose() = 
                if disposed = false then
                    disposed <- true
                    kernel.Dispose()
    /// <summary>
    /// A typed wrapper object for managing GPU Kernels
    /// </summary>
    type GpuKernel<'Arg, 'Result>(kernel : Kernel, args : seq<ParameterExpression>, 
                                                   query : QueryExpr,
                                                   compilerResult : Compiler.CompilerResult) = 
        inherit GpuKernel(kernel, args, query, compilerResult)
        
        

    /// <summary>
    /// A typed wrapper object for managing GPU Kernels
    /// </summary>
    type GpuKernel<'Arg1, 'Arg2, 'Result>(kernel : Kernel, args : seq<ParameterExpression>, 
                                                           query : QueryExpr,
                                                           compilerResult : Compiler.CompilerResult) = 
        inherit GpuKernel(kernel, args, query, compilerResult)

    /// <summary>
    /// A typed wrapper object for managing GPU Kernels
    /// </summary>
    type GpuKernel<'Arg1, 'Arg2, 'Arg3, 'Result>(kernel : Kernel, args : seq<ParameterExpression>, 
                                                                  query : QueryExpr,
                                                                  compilerResult : Compiler.CompilerResult) = 
        inherit GpuKernel(kernel, args, query, compilerResult)
