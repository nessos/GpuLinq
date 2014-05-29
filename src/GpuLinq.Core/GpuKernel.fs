namespace Nessos.GpuLinq.Core
    open System
    open System.Linq.Expressions;
    open OpenCL.Net.Extensions
    open OpenCL.Net

    /// <summary>
    /// Interface for managing GPU Kernels
    /// </summary>
    type IGpuKernel =
        inherit IDisposable
        abstract member Kernel : Kernel
        abstract member CompilerResult : Compiler.CompilerResult
        abstract member Args : seq<ParameterExpression>


    /// <summary>
    /// A base object for managing GPU Kernels
    /// </summary>
    type GpuKernel(kernel : Kernel, args : seq<ParameterExpression>, compilerResult : Compiler.CompilerResult) = 
        let mutable disposed = false
        interface IGpuKernel with 
                member self.Kernel = kernel
                member self.CompilerResult = compilerResult
                member self.Args = args
        interface System.IDisposable with 
            member this.Dispose() = 
                if disposed = false then
                    disposed <- true
                    kernel.Dispose()
    /// <summary>
    /// A typed wrapper object for managing GPU Kernels
    /// </summary>
    type GpuKernel<'Arg, 'Result>(kernel : Kernel, args : seq<ParameterExpression>, compilerResult : Compiler.CompilerResult) = 
        inherit GpuKernel(kernel, args, compilerResult)
        
        

    /// <summary>
    /// A typed wrapper object for managing GPU Kernels
    /// </summary>
    type GpuKernel<'Arg1, 'Arg2, 'Result>(kernel : Kernel, args : seq<ParameterExpression>, compilerResult : Compiler.CompilerResult) = 
        inherit GpuKernel(kernel, args, compilerResult)

    /// <summary>
    /// A typed wrapper object for managing GPU Kernels
    /// </summary>
    type GpuKernel<'Arg1, 'Arg2, 'Arg3, 'Result>(kernel : Kernel, args : seq<ParameterExpression>, compilerResult : Compiler.CompilerResult) = 
        inherit GpuKernel(kernel, args, compilerResult)
