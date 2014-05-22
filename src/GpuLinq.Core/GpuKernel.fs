namespace Nessos.GpuLinq.Core
    open System
    open OpenCL.Net.Extensions
    open OpenCL.Net

    /// <summary>
    /// Interface for managing GPU Kernels
    /// </summary>
    type IGpuKernel =
        inherit IDisposable
        abstract member GetKernel : unit -> Kernel

    /// <summary>
    /// A typed wrapper object for managing GPU Kernels
    /// </summary>
    type GpuKernel<'Arg, 'Result>(kernel : Kernel) = 
        let mutable disposed = false
        interface IGpuKernel with 
            member self.GetKernel () = kernel
        interface System.IDisposable with 
            member this.Dispose() = 
                if disposed = false then
                    disposed <- true
                    kernel.Dispose()

    /// <summary>
    /// A typed wrapper object for managing GPU Kernels
    /// </summary>
    type GpuKernel<'Arg1, 'Arg2, 'Result>(kernel : Kernel) = 
        let mutable disposed = false
        interface IGpuKernel with 
            member self.GetKernel () = kernel
        interface System.IDisposable with 
            member this.Dispose() = 
                if disposed = false then
                    disposed <- true
                    kernel.Dispose()

    /// <summary>
    /// A typed wrapper object for managing GPU Kernels
    /// </summary>
    type GpuKernel<'Arg1, 'Arg2, 'Arg3, 'Result>(kernel : Kernel) = 
        let mutable disposed = false
        interface IGpuKernel with 
            member self.GetKernel () = kernel
        interface System.IDisposable with 
            member this.Dispose() = 
                if disposed = false then
                    disposed <- true
                    kernel.Dispose()