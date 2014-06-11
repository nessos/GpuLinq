namespace Nessos.GpuLinq.Core
    open System
    open System.Linq
    open System.Linq.Expressions
    open System.Collections.Generic
    open System.Collections.Concurrent
    open System.Runtime.InteropServices
    open Nessos.LinqOptimizer.Core
    open Nessos.GpuLinq.Base
    open OpenCL.Net.Extensions
    open OpenCL.Net

    [<FlagsAttribute>]
    /// <summary>
    /// Options that can be used by the OpenCL compiler.
    /// </summary>
    type GpuCompileOptions =
        | FastRelaxedMath         = 1uy
        | FusedMultiplyAdd        = 2uy
        | DisableOptimizations    = 4uy
        | StrictAliasing          = 8uy
        | NoSignedZeros           = 16uy
        | UnsafeMathOptimizations = 32uy
        | FiniteMathOnly          = 64uy

    /// <summary>
    /// A scoped Context that manages GPU kernel execution and caching  
    /// </summary>
    type GpuContext(platformWildCard : string, compileOptions : GpuCompileOptions) =
        // Context environment
        let env = platformWildCard.CreateCLEnvironment()
        let cache = Dictionary<string, (Program * IGpuKernel)>()
        let buffers = new System.Collections.Generic.List<IGpuArray>()
        let maxGroupSize = 
                        match Cl.GetDeviceInfo(env.Devices.[0], DeviceInfo.MaxWorkGroupSize) with
                        | info, ErrorCode.Success -> info.CastTo<int>()
                        | _, error -> failwithf "OpenCL.GetDeviceInfo failed with error code %A" error
        
        let getLocalGroupSize (input : IGpuArray) = 
            if input.Length < maxGroupSize then input.Length 
            else maxGroupSize

        let compileOptionsToString (options : GpuCompileOptions) =
            let sb = new Text.StringBuilder()
            let contains pattern = options &&& pattern = pattern
            if contains GpuCompileOptions.FastRelaxedMath           then sb.Append(" -cl-fast-relaxed-math ")           |> ignore
            if contains GpuCompileOptions.FusedMultiplyAdd          then sb.Append(" -cl-mad-enable ")                  |> ignore
            if contains GpuCompileOptions.DisableOptimizations      then sb.Append(" -cl-opt-disable ")                 |> ignore
            if contains GpuCompileOptions.StrictAliasing            then sb.Append(" -cl-strict-aliasing ")             |> ignore
            if contains GpuCompileOptions.NoSignedZeros             then sb.Append(" -cl-no-signed-zeros ")             |> ignore
            if contains GpuCompileOptions.UnsafeMathOptimizations   then sb.Append(" -cl-unsafe-math-optimizations ")   |> ignore
            if contains GpuCompileOptions.FiniteMathOnly            then sb.Append(" -cl-finite-math-only ")            |> ignore
            sb.ToString()

        // helper functions
        let createBuffer (t : Type) (env : Environment) (length : int) =
            match Cl.CreateBuffer(env.Context, MemFlags.ReadWrite ||| MemFlags.None ||| MemFlags.AllocHostPtr, new IntPtr(length * Marshal.SizeOf(t))) with
            | inputBuffer, ErrorCode.Success -> inputBuffer
            | _, error -> failwithf "OpenCL.CreateBuffer failed with error code %A" error 
                
        let createGpuArray (t : Type) (env : Environment) (length : int) (capacity : int) (buffer : IMem) = 
            let gpuArray : IGpuArray = 
                match t with
                | TypeCheck Compiler.intType _ -> 
                    new GpuArray<int>(env, length, capacity, Marshal.SizeOf(t), buffer) :> _
                | TypeCheck Compiler.floatType _ ->  
                    new GpuArray<single>(env, length, capacity, Marshal.SizeOf(t), buffer) :> _
                | TypeCheck Compiler.doubleType _ ->  
                    new GpuArray<double>(env, length, capacity, Marshal.SizeOf(t), buffer) :> _
                | _ when t.IsValueType -> 
                    Activator.CreateInstance(typedefof<GpuArray<_>>.MakeGenericType [| t |], 
                                                [|env :> obj; length :> obj; capacity :> obj; Marshal.SizeOf(t) :> obj; buffer :> obj|]) :?> _
                | _ -> failwithf "Not supported result type %A" t
            buffers.Add(gpuArray)
            gpuArray

        let readFromBuffer (queue : CommandQueue) (t : Type) (outputBuffer : IMem) (output : obj) =
            match t with
            | TypeCheck Compiler.intType _ -> 
                let output = output :?> int[]
                queue.ReadFromBuffer(outputBuffer, output, 0, int64 output.Length)
            | TypeCheck Compiler.longType _ -> 
                let output = output :?> int64[]
                queue.ReadFromBuffer(outputBuffer, output, 0, int64 output.Length)
            | TypeCheck Compiler.floatType _ ->  
                let output = output :?> single[]
                queue.ReadFromBuffer(outputBuffer, output, 0, int64 output.Length)
            | TypeCheck Compiler.doubleType _ ->  
                let output = output :?> double[]
                queue.ReadFromBuffer(outputBuffer, output, 0, int64 output.Length)
            | TypeCheck Compiler.boolType _ ->  
                let output = output :?> bool[]
                queue.ReadFromBuffer(outputBuffer, output, 0, int64 output.Length)
            | _ -> failwithf "Not supported result type %A" t

        let createDynamicArray (t : Type) (flags : int[]) (output : obj) : Array =
            match t with
            | TypeCheck Compiler.intType _ ->
                let output = output :?> int[]
                let result = new System.Collections.Generic.List<int>(flags.Length)
                for i = 0 to flags.Length - 1 do
                    if flags.[i] = 0 then
                        result.Add(output.[i])
                let result = result.ToArray() 
                result :> _
            | TypeCheck Compiler.floatType _ ->  
                let output = output :?> float[]
                let result = new System.Collections.Generic.List<float>(flags.Length)
                for i = 0 to flags.Length - 1 do
                    if flags.[i] = 0 then
                        result.Add(output.[i])
                result.ToArray() :> _
            | TypeCheck Compiler.boolType _ ->  
                let output = output :?> bool[]
                let result = new System.Collections.Generic.List<bool>(flags.Length)
                for i = 0 to flags.Length - 1 do
                    if flags.[i] = 0 then
                        result.Add(output.[i])
                result.ToArray() :> _
            | _ -> failwithf "Not supported result type %A" t

        let createKernel(compilerResult : Compiler.CompilerResult) (createGpuKernel : Kernel -> IGpuKernel) : IGpuKernel =
            if cache.ContainsKey(compilerResult.Source) then
                let (_, kernel) = cache.[compilerResult.Source]
                kernel
            else
                let program, kernel = 
                    match Cl.CreateProgramWithSource(env.Context, 1u, [| compilerResult.Source |], null) with
                    | program, ErrorCode.Success ->
                        match Cl.BuildProgram(program, uint32 env.Devices.Length, env.Devices, compileOptionsToString compileOptions, null, IntPtr.Zero) with
                        | ErrorCode.Success -> 
                            match Cl.CreateKernel(program, "kernelCode") with
                            | kernel, ErrorCode.Success -> 
                                (program, kernel)
                            | _, error -> failwithf "OpenCL.CreateKernel failed with error code %A" error
                        | error -> failwithf "OpenCL.BuildProgram failed with error code %A" error
                    | _, error -> failwithf "OpenCL.CreateProgramWithSource failed with error code %A" error
                let kernel = createGpuKernel kernel 
                cache.Add(compilerResult.Source, (program, kernel))
                kernel

        let addKernelBufferArg (kernel : Kernel) (argIndex : int ref) (buffer : IMem) = 
            incr argIndex
            match Cl.SetKernelArg(kernel, uint32 !argIndex, buffer) with
            | ErrorCode.Success -> ()
            | error -> failwithf "OpenCL.SetKernelArg failed with error code %A" error

        let setKernelArg (kernel : Kernel) (argIndex : int ref)  (arg : 'T) = 
            incr argIndex
            match Cl.SetKernelArg(kernel, uint32 !argIndex, new IntPtr(sizeof<'T>), arg) with
            | ErrorCode.Success -> ()
            | error -> failwithf "OpenCL.SetKernelArg failed with error code %A" error

        let setKernelArgs (kernel : Kernel) (compilerResult : Compiler.CompilerResult) (argIndex : int ref) = 
            // Set Source Args
            for (_, input) in compilerResult.SourceArgs do
                if input.Length <> 0 then 
                    addKernelBufferArg kernel argIndex (input.GetBuffer()) 
                else incr argIndex 
                
            // Set Value Args
            for (paramExpr, value) in compilerResult.ValueArgs do
                match paramExpr.Type with
                | TypeCheck Compiler.intType _ | TypeCheck Compiler.floatType _ ->  
                    incr argIndex 
                    match Cl.SetKernelArg(kernel, uint32 !argIndex, new IntPtr(4), value) with
                    | ErrorCode.Success -> ()
                    | error -> failwithf "OpenCL.SetKernelArg failed with error code %A" error
                | Named(typedef, [|elemType|]) when typedef = typedefof<IGpuArray<_>> -> 
                    let gpuArray = (value :?> IGpuArray)
                    if gpuArray.Length <> 0 then
                        addKernelBufferArg kernel argIndex (gpuArray.GetBuffer()) 
                    else incr argIndex
                | _ -> failwithf "Not supported result type %A" paramExpr.Type

        new(platformWildCard : string) =
            new GpuContext(platformWildCard, GpuCompileOptions.FastRelaxedMath ||| GpuCompileOptions.FusedMultiplyAdd)
        new() =
            new GpuContext("*", GpuCompileOptions.FastRelaxedMath ||| GpuCompileOptions.FusedMultiplyAdd)

        /// <summary>
        /// Creates a GpuArray 
        /// </summary>
        /// <param name="gpuQuery">The array to be copied</param>
        /// <returns>A GpuArray object</returns>
        member self.CreateGpuArray<'T when 'T : struct and 'T : (new : unit -> 'T) and 'T :> ValueType>(array : 'T[]) : IGpuArray<'T> = 
            if array.Length = 0 then
                new GpuArray<'T>(env, 0, 0, sizeof<'T>, null) :> _
            else
                let capacity = if array.Length < maxGroupSize then maxGroupSize 
                               else int <| 2.0 ** Math.Ceiling(Math.Log(float array.Length, 2.0))
                let array' = Array.CreateInstance(typeof<'T>, capacity)
                Array.Copy(array, array', array.Length)
                match Cl.CreateBuffer(env.Context, MemFlags.ReadWrite ||| MemFlags.None ||| MemFlags.CopyHostPtr, new IntPtr(capacity * sizeof<'T>), array') with
                | inputBuffer, ErrorCode.Success -> 
                    let gpuArray = new GpuArray<'T>(array, env, array.Length, capacity, sizeof<'T>, inputBuffer) 
                    buffers.Add(gpuArray)
                    gpuArray :> _
                | _, error -> failwithf "OpenCL.CreateBuffer failed with error code %A" error 

        member private self.Compile (gpuQuery : Expression, f : Kernel -> seq<ParameterExpression> -> QueryExpr -> Compiler.CompilerResult -> IGpuKernel) : IGpuKernel = 
            match gpuQuery with
            | Lambda (args, bodyExpr) ->
                let queryExpr = Compiler.toQueryExpr bodyExpr
                let compilerResult = Compiler.compile queryExpr
                let kernel = createKernel compilerResult (fun kernel -> f kernel args queryExpr compilerResult)
                kernel 
            | _ -> failwithf "Invalid Expr %A" gpuQuery 
        /// <summary>
        /// Compiles QueryExpr and returns a GpuKernel handle object
        /// </summary>
        /// <param name="gpuQuery">The query to be copied</param>
        /// <returns>A GpuArray object</returns>
        member self.Compile (gpuQuery : Expression<Func<'Arg, IGpuQueryExpr<'Result>>>) : GpuKernel<'Arg, 'Result> = 
            self.Compile(gpuQuery, 
                            (fun kernel args queryExpr compilerResult -> new GpuKernel<'Arg, 'Result>(kernel, args, queryExpr, compilerResult) :> _)) :?> GpuKernel<'Arg, 'Result>
        /// <summary>
        /// Compiles QueryExpr and returns a GpuKernel handle object
        /// </summary>
        /// <param name="gpuQuery">The query to be copied</param>
        /// <returns>A GpuArray object</returns>
        member self.Compile (gpuQuery : Expression<Func<'Arg1, 'Arg2, IGpuQueryExpr<'Result>>>) : GpuKernel<'Arg1, 'Arg2, 'Result> = 
            self.Compile(gpuQuery, 
                            (fun kernel args queryExpr compilerResult -> new GpuKernel<'Arg1, 'Arg2, 'Result>(kernel, args, queryExpr, compilerResult) :> _)) :?> GpuKernel<'Arg1, 'Arg2, 'Result>
        /// <summary>
        /// Compiles QueryExpr and returns a GpuKernel handle object
        /// </summary>
        /// <param name="gpuQuery">The query to be copied</param>
        /// <returns>A GpuArray object</returns>
        member self.Compile (gpuQuery : Expression<Func<'Arg1, 'Arg2, 'Arg3, IGpuQueryExpr<'Result>>>) : GpuKernel<'Arg1, 'Arg2, 'Arg3, 'Result> = 
            self.Compile(gpuQuery, 
                            (fun kernel args queryExpr compilerResult -> new GpuKernel<'Arg1, 'Arg2, 'Arg3, 'Result>(kernel, args, queryExpr, compilerResult) :> _)) :?> GpuKernel<'Arg1, 'Arg2, 'Arg3, 'Result>



        member private self.Fill(kernel : Kernel, queryExpr : QueryExpr, compilerResult : Compiler.CompilerResult, outputGpuArray : IGpuArray) : unit = 
            // Set Kernel Args
            let argIndex = ref -1
            setKernelArgs kernel compilerResult argIndex
            // Prepare query and execute 
            match compilerResult.ReductionType with
            | ReductionType.Map -> 
                let (_, input) = compilerResult.SourceArgs.[0]
                if input.Length = 0 then
                    ()
                else
                    match compilerResult.SourceType with
                    | Compiler.SourceType.NestedSource ->
                        let (_, nestedInput) = compilerResult.SourceArgs.[1]
                        setKernelArg kernel argIndex input.Length    
                        setKernelArg kernel argIndex nestedInput.Length
                        addKernelBufferArg kernel argIndex (outputGpuArray.GetBuffer())
                        match Cl.EnqueueNDRangeKernel(env.CommandQueues.[0], kernel, uint32 1, null, [| new IntPtr(input.Length) |], [| new IntPtr(getLocalGroupSize input) |], uint32 0, null) with
                        | ErrorCode.Success, event ->
                            use event = event
                            env.CommandQueues.[0].Finish() |> ignore
                        | _, error -> failwithf "OpenCL.EnqueueNDRangeKernel failed with error code %A" error
                    | Compiler.SourceType.SingleSource | Compiler.SourceType.Zip ->
                        addKernelBufferArg kernel argIndex (outputGpuArray.GetBuffer())
                        match Cl.EnqueueNDRangeKernel(env.CommandQueues.[0], kernel, uint32 1, null, [| new IntPtr(input.Capacity) |], [| new IntPtr(maxGroupSize) |], uint32 0, null) with
                        | ErrorCode.Success, event ->
                            use event = event
                            env.CommandQueues.[0].Finish() |> ignore
                        | _, error -> failwithf "OpenCL.EnqueueNDRangeKernel failed with error code %A" error
                    
                    
            | reductionType -> failwith "Invalid ReductionType %A" reductionType

        member private self.Fill(kernel : IGpuKernel, outputGpuArray : IGpuArray, args : obj[]) : unit = 
            let sourceArgs = Array.copy kernel.CompilerResult.SourceArgs
            let valueArgs = Array.copy kernel.CompilerResult.ValueArgs
            (kernel.Args, args)
            ||> Seq.zip
            |> Seq.iter (fun (kernelArg, valueArg) -> 
                kernel.CompilerResult.SourceArgs 
                |> Seq.iteri (fun i (sourceArg, gpuArray) -> 
                    if sourceArg = kernelArg && gpuArray = Unchecked.defaultof<IGpuArray> then
                         sourceArgs.[i] <- (sourceArg, valueArg :?> IGpuArray))

                kernel.CompilerResult.ValueArgs
                |> Seq.iteri (fun i (sourceArg, value) -> 
                    if sourceArg = kernelArg && value = null then
                         valueArgs.[i] <- (sourceArg, valueArg))
                )
            self.Fill(kernel.Kernel, kernel.Query, { kernel.CompilerResult with SourceArgs = sourceArgs; ValueArgs = valueArgs }, outputGpuArray)

        /// <summary>
        /// Runs the kernel and fills with data the output gpuArray.
        /// </summary>
        /// <param name="kernel">The query to run.</param>
        /// <param name="outputGpuArray">The gpuArray to be filled.</param>
        /// <param name="arg">Kernel argument.</param>
        /// <returns>void</returns>            
        member self.Fill<'Arg, 'Result>(kernel : GpuKernel<'Arg, IGpuArray<'Result>>, outputGpuArray : IGpuArray<'Result>, arg : 'Arg) : unit =
            self.Fill(kernel, outputGpuArray, [|arg :> obj|])

        /// <summary>
        /// Runs the kernel and fills with data the output gpuArray.
        /// </summary>
        /// <param name="kernel">The kernel to run.</param>
        /// <param name="arg1">Kernel argument1.</param>
        /// <param name="arg2">Kernel argument2.</param>
        /// <param name="arg3">Kernel argument2.</param>
        /// <returns>void</returns>            
        member self.Fill<'Arg1, 'Arg2, 'Arg3, 'Result> (kernel : GpuKernel<'Arg1, 'Arg2, 'Arg3, IGpuArray<'Result>>, outputGpuArray : IGpuArray<'Result>, arg1 : 'Arg1, arg2 : 'Arg2, arg3 : 'Arg3) : unit =
            self.Fill(kernel :> IGpuKernel, outputGpuArray, [|arg1 :> obj; arg2 :> obj; arg3 :> obj|])
                        
        /// <summary>
        /// Runs the kernel and fills with data the output gpuArray.
        /// </summary>
        /// <param name="kernel">The kernel to run.</param>
        /// <param name="arg1">Kernel argument1.</param>
        /// <param name="arg2">Kernel argument2.</param>
        /// <returns>void</returns>            
        member self.Fill<'Arg1, 'Arg2, 'Result> (kernel : GpuKernel<'Arg1, 'Arg2, IGpuArray<'Result>>, outputGpuArray : IGpuArray<'Result>, arg1 : 'Arg1, arg2 : 'Arg2) : unit =
            self.Fill(kernel :> IGpuKernel, outputGpuArray, [|arg1 :> obj; arg2 :> obj|]) 

        /// <summary>
        /// Compiles a gpu query to gpu kernel code, runs the kernel and fills with data the output gpuArray.
        /// </summary>
        /// <param name="gpuQuery">The query to run.</param>
        /// <param name="outputGpuArray">The gpuArray to be filled.</param>
        /// <returns>void</returns>            
        member self.Fill<'T>(gpuQuery : IGpuQueryExpr<IGpuArray<'T>>, outputGpuArray : IGpuArray<'T>) : unit =
            let queryExpr = gpuQuery.Expr
            let compilerResult = Compiler.compile queryExpr
            let kernel = (createKernel compilerResult (fun kernel -> new GpuKernel(kernel, [||], queryExpr, compilerResult) :> _)).Kernel
            self.Fill(kernel, queryExpr, compilerResult, outputGpuArray) 


        member private self.Run(kernel : Kernel, queryExpr : QueryExpr, compilerResult : Compiler.CompilerResult) : obj = 
            // Set Kernel Args
            let argIndex = ref -1
            setKernelArgs kernel compilerResult argIndex

            // Prepare query and execute 
            match compilerResult.ReductionType with
            | ReductionType.NestedQueryTransform -> 
                let (_, input), (_, nestedInput) = compilerResult.SourceArgs.[0], compilerResult.SourceArgs.[1]
                let gpuArray = 
                    if input.Length = 0 then
                        createGpuArray queryExpr.Type env input.Length input.Capacity null 
                    else
                        setKernelArg kernel argIndex input.Length
                        setKernelArg kernel argIndex nestedInput.Length
                        let outputBuffer = createBuffer queryExpr.Type env (input.Length * nestedInput.Length)
                        addKernelBufferArg kernel argIndex outputBuffer 
                        match Cl.EnqueueNDRangeKernel(env.CommandQueues.[0], kernel, uint32 1, null, [| new IntPtr(input.Length) |], [| new IntPtr(getLocalGroupSize input) |], uint32 0, null) with
                        | ErrorCode.Success, event ->
                            use event = event
                            env.CommandQueues.[0].Finish() |> ignore
                            createGpuArray queryExpr.Type env (input.Length * nestedInput.Length) (input.Capacity * nestedInput.Capacity) outputBuffer 
                        | _, error -> failwithf "OpenCL.EnqueueNDRangeKernel failed with error code %A" error
                match queryExpr with
                | ToArray (_) -> 
                    use gpuArray = gpuArray
                    gpuArray.ToArray() :> obj 
                | _ -> gpuArray :> obj 
            | ReductionType.Map -> 
                let (_, input) = compilerResult.SourceArgs.[0]
                let gpuArray = 
                    if input.Length = 0 then
                        createGpuArray queryExpr.Type env input.Length input.Capacity null 
                    else
                        match compilerResult.SourceType with
                        | Compiler.SingleSource | Compiler.Zip ->
                            let outputBuffer = createBuffer queryExpr.Type env input.Capacity
                            addKernelBufferArg kernel argIndex outputBuffer 
                            match Cl.EnqueueNDRangeKernel(env.CommandQueues.[0], kernel, uint32 1, null, [| new IntPtr(input.Capacity) |], [| new IntPtr(maxGroupSize) |], uint32 0, null) with
                            | ErrorCode.Success, event ->
                                use event = event
                                env.CommandQueues.[0].Finish() |> ignore
                                createGpuArray queryExpr.Type env input.Length input.Capacity outputBuffer 
                            | _, error -> failwithf "OpenCL.EnqueueNDRangeKernel failed with error code %A" error
                        | Compiler.NestedSource ->
                            let (_, nestedInput) = compilerResult.SourceArgs.[1]
                            setKernelArg kernel argIndex input.Length
                            setKernelArg kernel argIndex nestedInput.Length
                            let outputBuffer = createBuffer queryExpr.Type env (input.Capacity * nestedInput.Capacity)
                            addKernelBufferArg kernel argIndex outputBuffer 
                            match Cl.EnqueueNDRangeKernel(env.CommandQueues.[0], kernel, uint32 1, null, [| new IntPtr(input.Length) |], [| new IntPtr(getLocalGroupSize input) |], uint32 0, null) with
                            | ErrorCode.Success, event ->
                                use event = event
                                env.CommandQueues.[0].Finish() |> ignore
                                createGpuArray queryExpr.Type env (input.Length * nestedInput.Length) (input.Capacity * nestedInput.Capacity) outputBuffer 
                            | _, error -> failwithf "OpenCL.EnqueueNDRangeKernel failed with error code %A" error
                match queryExpr with
                | ToArray (_) -> 
                    use gpuArray = gpuArray
                    gpuArray.ToArray() :> obj 
                | _ -> gpuArray :> obj 
            | ReductionType.Filter -> 
                let (_, input) = compilerResult.SourceArgs.[0]
                let gpuArray = 
                    if input.Length = 0 then
                        createGpuArray queryExpr.Type env input.Length input.Capacity null 
                    else
                        let output = Array.CreateInstance(queryExpr.Type, input.Length)
                        use outputBuffer = createBuffer queryExpr.Type env input.Capacity
                        let flags = Array.CreateInstance(typeof<int>, input.Length)
                        use flagsBuffer = createBuffer typeof<int> env input.Capacity
                        addKernelBufferArg kernel argIndex flagsBuffer 
                        addKernelBufferArg kernel argIndex outputBuffer 
                        match Cl.EnqueueNDRangeKernel(env.CommandQueues.[0], kernel, uint32 1, null, [| new IntPtr(input.Capacity) |], [| new IntPtr(maxGroupSize) |], uint32 0, null) with
                        | ErrorCode.Success, event ->
                            use event = event
                            readFromBuffer env.CommandQueues.[0] typeof<int> flagsBuffer flags
                            readFromBuffer env.CommandQueues.[0] queryExpr.Type outputBuffer output 
                            let result = createDynamicArray queryExpr.Type (flags :?> int[]) output
                            match Cl.CreateBuffer(env.Context, MemFlags.ReadWrite ||| MemFlags.None ||| MemFlags.UseHostPtr, new IntPtr(input.Length * input.Size), result) with
                            | resultBuffer, ErrorCode.Success ->
                                createGpuArray queryExpr.Type env result.Length input.Capacity resultBuffer 
                            | _, error -> failwithf "OpenCL.CreateBuffer failed with error code %A" error 
                        | _, error -> failwithf "OpenCL.EnqueueNDRangeKernel failed with error code %A" error
                match queryExpr with
                | ToArray (_) -> 
                    use gpuArray = gpuArray
                    gpuArray.ToArray() :> obj 
                | _ -> gpuArray :> obj 
            | ReductionType.Sum | ReductionType.Count ->
                let (_, gpuArray)  = compilerResult.SourceArgs.[0]
                if gpuArray.Length = 0 then
                    match queryExpr.Type with
                    | TypeCheck Compiler.intType _ -> 0 :> _
                    | TypeCheck Compiler.longType _ -> 0L :> _
                    | TypeCheck Compiler.floatType _ -> 0.0f :> _
                    | TypeCheck Compiler.doubleType _ -> 0.0 :> _
                    | _ -> failwithf "Not supported %A" queryExpr.Type 
                else
                    let maxGroupSize = 
                        match Cl.GetKernelWorkGroupInfo(kernel, env.Devices.[0], KernelWorkGroupInfo.WorkGroupSize) with
                        | info, ErrorCode.Success -> info.CastTo<int>()
                        | error -> failwithf "OpenCL.GetKernelWorkGroupInfo failed with error code %A" error
                    let (maxGroupSize, outputLength) = if gpuArray.Capacity < maxGroupSize then (gpuArray.Capacity, 1)
                                                       else (maxGroupSize, gpuArray.Capacity / maxGroupSize)

                    match Cl.SetKernelArg(kernel, (incr argIndex; uint32 !argIndex), new IntPtr(sizeof<int>), gpuArray.Length) with
                    | ErrorCode.Success -> ()
                    | error -> failwithf "OpenCL.SetKernelArg failed with error code %A" error
                    let output = Array.CreateInstance(queryExpr.Type, outputLength)
                    use outputBuffer = createBuffer queryExpr.Type env outputLength
                    addKernelBufferArg kernel argIndex outputBuffer 
                    // local buffer
                    match Cl.SetKernelArg(kernel, (incr argIndex; uint32 !argIndex), new IntPtr(Marshal.SizeOf(queryExpr.Type) * maxGroupSize), null) with
                    | ErrorCode.Success ->
                        match Cl.EnqueueNDRangeKernel(env.CommandQueues.[0], kernel, uint32 1, null, [| new IntPtr(gpuArray.Capacity) |], [| new IntPtr(maxGroupSize) |], uint32 0, null) with
                        | ErrorCode.Success, event ->
                            use event = event
                            readFromBuffer env.CommandQueues.[0] queryExpr.Type outputBuffer output 
                            match queryExpr.Type with
                            | TypeCheck Compiler.intType _ ->
                                (output :?> int[]).Sum() :> obj  
                            | TypeCheck Compiler.longType _ ->
                                (output :?> int64[]).Sum() :> obj  
                            | TypeCheck Compiler.floatType _ ->  
                                (output :?> System.Single[]).Sum() :> obj   
                            | TypeCheck Compiler.doubleType _ ->  
                                (output :?> System.Double[]).Sum() :> obj   
                            | t -> failwithf "Not supported result type %A" t
                        | error, _  -> failwithf "OpenCL.EnqueueNDRangeKernel failed with error code %A" error
                    | error -> failwithf "OpenCL.SetKernelArg failed with error code %A" error
            | reductionType -> failwithf "Invalid ReductionType %A" reductionType


        member private self.Run(kernel : IGpuKernel, args : obj[]) : obj = 
            let sourceArgs = Array.copy kernel.CompilerResult.SourceArgs
            let valueArgs = Array.copy kernel.CompilerResult.ValueArgs
            (kernel.Args, args)
            ||> Seq.zip
            |> Seq.iter (fun (kernelArg, valueArg) -> 
                kernel.CompilerResult.SourceArgs 
                |> Seq.iteri (fun i (sourceArg, gpuArray) -> 
                    if sourceArg = kernelArg && gpuArray = Unchecked.defaultof<IGpuArray> then
                         sourceArgs.[i] <- (sourceArg, valueArg :?> IGpuArray))

                kernel.CompilerResult.ValueArgs
                |> Seq.iteri (fun i (sourceArg, value) -> 
                    if sourceArg = kernelArg && value = null then
                         valueArgs.[i] <- (sourceArg, valueArg))
                )
            self.Run(kernel.Kernel, kernel.Query, { kernel.CompilerResult with SourceArgs = sourceArgs; ValueArgs = valueArgs })


        /// <summary>
        /// Runs the kernel and returns the result.
        /// </summary>
        /// <param name="kernel">The kernel to run.</param>
        /// <param name="arg1">Kernel argument1.</param>
        /// <param name="arg2">Kernel argument2.</param>
        /// <param name="arg3">Kernel argument2.</param>
        /// <returns>The result of the kernel.</returns>
        member self.Run<'Arg1, 'Arg2, 'Arg3, 'Result> (kernel : GpuKernel<'Arg1, 'Arg2, 'Arg3, 'Result>, arg1 : 'Arg1, arg2 : 'Arg2, arg3 : 'Arg3) : 'Result =
            self.Run(kernel :> IGpuKernel, [|arg1 :> obj; arg2 :> obj; arg3 :> obj|]) :?> _
                        
        /// <summary>
        /// Runs the kernel and returns the result.
        /// </summary>
        /// <param name="kernel">The kernel to run.</param>
        /// <param name="arg1">Kernel argument1.</param>
        /// <param name="arg2">Kernel argument2.</param>
        /// <returns>The result of the kernel.</returns>
        member self.Run<'Arg1, 'Arg2, 'Result> (kernel : GpuKernel<'Arg1, 'Arg2, 'Result>, arg1 : 'Arg1, arg2 : 'Arg2) : 'Result =
            self.Run(kernel :> IGpuKernel, [|arg1 :> obj; arg2 :> obj|]) :?> _

        /// <summary>
        /// Runs the kernel and returns the result.
        /// </summary>
        /// <param name="kernel">The kernel to run.</param>
        /// <param name="arg">Kernel argument.</param>
        /// <returns>The result of the kernel.</returns>
        member self.Run<'Arg, 'Result> (kernel : GpuKernel<'Arg, 'Result>, arg : 'Arg) : 'Result =
            self.Run(kernel :> IGpuKernel, [|arg :> obj|]) :?> _

            

        /// <summary>
        /// Compiles a gpu query to gpu kernel code, runs the kernel and returns the result.
        /// </summary>
        /// <param name="gpuQuery">The query to run.</param>
        /// <returns>The result of the query.</returns>
        member self.Run<'TQuery> (gpuQuery : IGpuQueryExpr<'TQuery>) : 'TQuery =
            let queryExpr = gpuQuery.Expr
            let compilerResult = Compiler.compile queryExpr
            let kernel = (createKernel compilerResult (fun kernel -> new GpuKernel(kernel, [||], queryExpr, compilerResult) :> _)).Kernel
            self.Run(kernel, queryExpr, compilerResult) :?> _
            


        interface System.IDisposable with 
            member this.Dispose() = 
                buffers |> Seq.iter (fun displosable -> try displosable.Dispose() with _ -> ())
                let disposable = cache |> Seq.map (fun (keyValue : KeyValuePair<_, _>) -> let program, kernel = keyValue.Value in [program :> IDisposable; kernel :> IDisposable]) |> Seq.concat |> Seq.toList
                (env :> IDisposable) :: disposable |> Seq.iter (fun displosable -> try displosable.Dispose() with _ -> ())
                
                
                        
            
            

