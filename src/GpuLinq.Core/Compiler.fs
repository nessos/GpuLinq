namespace Nessos.GpuLinq.Core
    open System
    open System.Linq
    open System.Linq.Expressions
    open System.Runtime.InteropServices
    open OpenCL.Net
    open Nessos.LinqOptimizer.Core
    open Nessos.LinqOptimizer.Core.Utils

    module internal Compiler =
        type Length = int
        type Size = int

        type QueryContext = { CurrentVarExpr : ParameterExpression; AccVarExpr : ParameterExpression; FlagVarExpr : ParameterExpression;
                                BreakLabel : LabelTarget; ContinueLabel : LabelTarget;
                                InitExprs : Expression list; AccExpr : Expression; CombinerExpr : Expression; ResultType : Type; 
                                VarExprs : ParameterExpression list; Exprs : Expression list; ReductionType : ReductionType }
        
        type CompilerResult = { Source : string; ReductionType : ReductionType; SourceArgs : IGpuArray []; ValueArgs : (obj * Type) [] }
        
        let intType = typeof<int>
        let floatType = typeof<single>
        let doubleType = typeof<double>
        let byteType = typeof<byte>
        let boolType = typeof<bool>
        let gpuArrayTypeDef = typedefof<GpuArray<_>>

        let breakLabel () = labelTarget "brk"
        let continueLabel () = labelTarget "cont"

        let rec compile (queryExpr : QueryExpr) : CompilerResult = 

            let rec compile' (queryExpr : QueryExpr) (context : QueryContext) =
                let rec typeToStr (t : Type) = 
                    match t with
                    | TypeCheck intType _ -> "int"
                    | TypeCheck floatType _ -> "float"
                    | TypeCheck doubleType _ -> "double"
                    | TypeCheck byteType _ -> "byte"
                    | Named(typedef, [|elemType|]) when typedef = typedefof<IGpuArray<_>> -> 
                        sprintf' "__global %s*" (typeToStr elemType)
                    | _ when t.IsValueType && not t.IsPrimitive -> t.Name                    
                    | _ -> failwithf "Not supported %A" t

                let varExprToStr (varExpr : ParameterExpression) (vars : seq<ParameterExpression>) = 
                    let index = vars |> Seq.findIndex (fun varExpr' -> varExpr = varExpr')
                    sprintf' "%s%d" (varExpr.ToString()) index
                let constantLifting (exprs : Expression list) = 
                    match exprs with
                    | [] -> ([||], [||], [||])
                    | _ ->
                        let expr, paramExprs, objs = ConstantLiftingTransformer.apply (block [] exprs) 
                        ((expr :?> BlockExpression).Expressions.ToArray(), paramExprs, objs)
                let isCustomStruct (t : Type) = t.IsValueType && not t.IsPrimitive
                let structToStr (t : Type) = 
                    let fieldsStr = 
                        (t.GetFields(), "")
                        ||> Array.foldBack (fun fieldInfo fieldsStr -> sprintf' "%s %s; %s" (typeToStr fieldInfo.FieldType) fieldInfo.Name fieldsStr) 
                    sprintf' "typedef struct { %s } %s;" fieldsStr t.Name
                let collectCustomStructs (expr : Expression) = 
                    match expr with
                    | AnonymousTypeAssign(_ ,AnonymousTypeConstruction(members, args)) 
                        when isCustomStruct <| args.Last().Type -> 
                        [args.Last().Type] 
                    | Assign (Parameter (paramExpr), expr') when isCustomStruct paramExpr.Type -> 
                        [paramExpr.Type]
                    | _ when isCustomStruct expr.Type -> [expr.Type]
                    | _ -> []
                let customStructsToStr (types : seq<Type>) = 
                    types 
                    |> Seq.map structToStr
                    |> Set.ofSeq
                    |> Seq.fold (fun first second -> sprintf' "%s%s%s" first Environment.NewLine second) ""
                let structsDefinitionStr (exprs : seq<Expression>) =
                    exprs
                    |> Seq.collect collectCustomStructs
                    |> customStructsToStr
                let argsToStr (argParamExprs : ParameterExpression[]) (paramExprs : seq<ParameterExpression>) = 
                    let argParamExprs = argParamExprs |> Array.filter (fun paramExpr -> not <| typeof<Expression>.IsAssignableFrom(paramExpr.Type))
                    (argParamExprs, "") 
                    ||> Array.foldBack (fun paramExpr result -> sprintf' "%s %s, %s" (typeToStr paramExpr.Type) (varExprToStr paramExpr paramExprs) result) 

                let rec exprToStr (expr : Expression) (vars : seq<ParameterExpression>) =
                    match expr with
                    // AnonymousType handling
                    | AnonymousTypeMember expr ->
                        match vars |> Seq.tryFind (fun varExpr -> varExpr.Name = expr.Member.Name) with
                        | Some varExpr -> varExprToStr varExpr vars
                        | None -> expr.Member.Name
                    | AnonymousTypeAssign(_ , AnonymousTypeConstruction(members, args)) ->
                        sprintf' "%s %s = %s" (typeToStr <| args.Last().Type) (members.Last().Name) (exprToStr <| args.Last() <| vars)
                    | FieldMember (expr, fieldMember) -> sprintf' "%s.%s" (exprToStr expr vars) fieldMember.Name
                    | MethodCall (objExpr, methodInfo, [argExpr]) when methodInfo.Name = "get_Item" ->
                        sprintf' "%s[%s]" (exprToStr objExpr vars) (exprToStr argExpr vars)
                    // Math functions
                    | MethodCall (objExpr, methodInfo, [argExpr]) when methodInfo.Name = "Cos" ->
                        sprintf' "cos(%s)" (exprToStr argExpr vars)
                    | MethodCall (objExpr, methodInfo, [argExpr]) when methodInfo.Name = "Sin" ->
                        sprintf' "sin(%s)" (exprToStr argExpr vars)
                    | MethodCall (objExpr, methodInfo, [argExpr]) when methodInfo.Name = "Floor" ->
                        sprintf' "floor(%s)" (exprToStr argExpr vars)
                    | MethodCall (objExpr, methodInfo, [argExpr]) when methodInfo.Name = "Sqrt" ->
                        sprintf' "sqrt(%s)" (exprToStr argExpr vars)
                    | MethodCall (objExpr, methodInfo, [argExpr]) when methodInfo.Name = "Exp" ->
                        sprintf' "exp(%s)" (exprToStr argExpr vars)
                    | MethodCall (objExpr, methodInfo, [firstExpr; secondExpr]) when methodInfo.Name = "Pow" ->
                        sprintf' "powr(%s, %s)" (exprToStr firstExpr vars) (exprToStr secondExpr vars)
                    | MethodCall (objExpr, methodInfo, [argExpr]) when methodInfo.Name = "Log" ->
                        sprintf' "log(%s)" (exprToStr argExpr vars)
                    | MethodCall (objExpr, methodInfo, [argExpr]) when methodInfo.Name = "Abs" && 
                        (methodInfo.ReturnType = typeof<double> || methodInfo.ReturnType = typeof<Single>) ->
                        sprintf' "fabs(%s)" (exprToStr argExpr vars)

                    // Expr Call
                    | MethodCall (objExpr, methodInfo, [Parameter funcExpr; argExpr]) when typeof<Expression>.IsAssignableFrom(funcExpr.Type) &&
                                                                                 methodInfo.Name = "Invoke" ->
                        sprintf' "%s(%s)" funcExpr.Name (exprToStr argExpr vars)
                    | ValueTypeMemberInit (members, bindings) ->
                        let bindingsStr = bindings |> Seq.fold (fun bindingsStr binding -> sprintf' ".%s = %s, %s" binding.Member.Name (exprToStr binding.Expression vars) bindingsStr) ""
                        sprintf' "(%s) { %s }" (typeToStr expr.Type) bindingsStr
                    | Constant (value, TypeCheck intType _) -> sprintf' "%A" value
                    | Constant (value, TypeCheck floatType _) -> sprintf' "%A" value
                    | Constant (value, TypeCheck doubleType _) -> sprintf' "%A" value
                    | Constant (value, TypeCheck byteType _) -> sprintf' "%A" value
                    | Constant (value, TypeCheck boolType _) -> sprintf' "%A" value
                    | Parameter (paramExpr) -> varExprToStr paramExpr vars
                    | Assign (Parameter (paramExpr), expr') -> sprintf' "%s = %s" (varExprToStr paramExpr vars) (exprToStr expr' vars)
                    | Plus (leftExpr, rightExpr) -> sprintf' "(%s + %s)" (exprToStr leftExpr vars) (exprToStr rightExpr vars)
                    | Subtract (leftExpr, rightExpr) -> sprintf' "(%s - %s)" (exprToStr leftExpr vars) (exprToStr rightExpr vars)
                    | Times (leftExpr, rightExpr) -> sprintf' "(%s * %s)" (exprToStr leftExpr vars) (exprToStr rightExpr vars)
                    | Divide (leftExpr, rightExpr) -> sprintf' "(%s / %s)" (exprToStr leftExpr vars) (exprToStr rightExpr vars)
                    | Modulo (leftExpr, rightExpr) -> sprintf' "(%s %% %s)" (exprToStr leftExpr vars) (exprToStr rightExpr vars)
                    | Negate (expr) -> sprintf' "(-%s)" (exprToStr expr vars)
                    | Equal (leftExpr, rightExpr) -> sprintf' "(%s == %s)" (exprToStr leftExpr vars) (exprToStr rightExpr vars)
                    | NotEqual (leftExpr, rightExpr) -> sprintf' "(%s != %s)" (exprToStr leftExpr vars) (exprToStr rightExpr vars)
                    | GreaterThan (leftExpr, rightExpr) -> sprintf' "(%s > %s)" (exprToStr leftExpr vars) (exprToStr rightExpr vars)
                    | GreaterThanOrEqual (leftExpr, rightExpr) -> sprintf' "(%s >= %s)" (exprToStr leftExpr vars) (exprToStr rightExpr vars)
                    | LessThan (leftExpr, rightExpr) -> sprintf' "(%s < %s)" (exprToStr leftExpr vars) (exprToStr rightExpr vars)
                    | LessThanOrEqual (leftExpr, rightExpr) -> sprintf' "(%s <= %s)" (exprToStr leftExpr vars) (exprToStr rightExpr vars)
                    | IfThenElse (testExpr, thenExpr, elseExpr) -> 
                        if thenExpr.NodeType = ExpressionType.Block && elseExpr.NodeType = ExpressionType.Block then
                            sprintf' "if (%s) { %s; } else { %s; }" (exprToStr testExpr vars) (exprToStr thenExpr vars) (exprToStr elseExpr vars)
                        else
                            sprintf' "((%s) ? %s : %s)" (exprToStr testExpr vars) (exprToStr thenExpr vars) (exprToStr elseExpr vars)
                    | Goto (kind, target, value) when kind = GotoExpressionKind.Continue -> sprintf' "goto %s" target.Name 
                    | Block (_, exprs, _) -> 
                        exprs
                            |> Seq.map (fun expr -> sprintf' "%s" (exprToStr expr vars))
                            |> Seq.reduce (fun first second -> sprintf' "%s;%s%s" first Environment.NewLine second)
                    | Convert (expr, t) -> sprintf' "((%s) %s)" (typeToStr t) (exprToStr expr vars)
                    | Nop _ -> ""
                    | _ -> failwithf "Not supported %A" expr

                let rec collectFuncsStr (paramExprs : ParameterExpression[], values : obj[]) = 
                    let funcsStr = 
                        (paramExprs, values) 
                        ||> Array.zip 
                        |> Array.map (fun (paramExpr, value) -> 
                                        match value with
                                        | :? Expression as expr -> 
                                            match expr with
                                            | Lambda ([varExpr], bodyExpr) -> 
                                                let expr, paramExprs, objs = ConstantLiftingTransformer.apply bodyExpr
                                                let exprStr = exprToStr expr [varExpr]
                                                let funcStr = sprintf' "inline %s(%s %s) { return %s; }%s %s" 
                                                                paramExpr.Name (typeToStr varExpr.Type) (varExprToStr varExpr [varExpr]) exprStr Environment.NewLine
                                                                (collectFuncsStr (paramExprs, objs))
                                                funcStr
                                            | _ -> "" 
                                        | _ -> "")
                        |> String.concat Environment.NewLine
                    funcsStr
                let headerStr (exprs : Expression[], paramExprs : ParameterExpression[], values : obj[]) = 
                    let funcsStr = collectFuncsStr (paramExprs, values)
                    let structsStr = structsDefinitionStr exprs
                    [KernelTemplates.openCLExtensions; funcsStr; structsStr] |> String.concat Environment.NewLine

                let bodyStr (exprs : seq<Expression>) (vars : seq<ParameterExpression>) =
                    let exprsStr = exprs
                                       |> Seq.map (fun expr -> sprintf' "%s;" (exprToStr expr vars))
                                       |> Seq.fold (fun first second -> sprintf' "%s%s%s" first Environment.NewLine second) ""
                    let varsStr = vars
                                      |> Seq.filter (fun varExpr -> not (varExpr.Name.StartsWith("___param")))
                                      |> Seq.filter (fun varExpr -> not (isAnonymousType varExpr.Type))
                                      |> Seq.map (fun varExpr -> sprintf' "%s %s;" (typeToStr varExpr.Type) (varExprToStr varExpr vars)) 
                                      |> Seq.fold (fun first second -> sprintf' "%s%s%s" first Environment.NewLine second) "" 
                    (exprsStr, varsStr)

                let collectValueArgs (paramExprs : ParameterExpression[], values : obj[]) = 
                    (paramExprs, values) 
                    ||> Array.zip 
                    |> Array.filter (fun (paramExpr, _) -> not (typeof<Expression>.IsAssignableFrom(paramExpr.Type)))
                    |> Array.map (fun (paramExpr, value) -> (value, paramExpr.Type))

                match queryExpr with
                | Source (Constant (value, Named (TypeCheck gpuArrayTypeDef _, [|_|])) as expr, sourceType, QueryExprType.Gpu) ->
                    let sourceTypeStr = typeToStr sourceType
                    let resultTypeStr = typeToStr context.ResultType
                    let gpuArraySource = value :?> IGpuArray
                    let sourceLength = gpuArraySource.Length
                    let exprs, paramExprs, values = constantLifting context.Exprs
                    let vars = Seq.append paramExprs context.VarExprs
                    let headerStr = headerStr (exprs, paramExprs, values)
                    let valueArgs = collectValueArgs (paramExprs, values) 
                    let (exprsStr, varsStr) = bodyStr exprs vars
                    let argsStr = argsToStr paramExprs vars
                    match context.ReductionType with
                    | ReductionType.Map ->
                        let source = KernelTemplates.mapTemplate headerStr sourceTypeStr argsStr resultTypeStr varsStr (varExprToStr context.CurrentVarExpr vars) exprsStr (varExprToStr context.AccVarExpr vars)
                        { Source = source; ReductionType = context.ReductionType; SourceArgs = [| gpuArraySource |]; ValueArgs = valueArgs  }
                    | ReductionType.Filter ->
                        let source = KernelTemplates.mapFilterTemplate headerStr sourceTypeStr argsStr resultTypeStr varsStr (varExprToStr context.CurrentVarExpr vars) exprsStr (varExprToStr context.FlagVarExpr vars) (varExprToStr context.AccVarExpr vars) 
                        { Source = source; ReductionType = context.ReductionType; SourceArgs = [| (value :?> IGpuArray) |]; ValueArgs = valueArgs }
                    | ReductionType.Sum | ReductionType.Count -> 
                        let gpuArray = value :?> IGpuArray
                        let source = KernelTemplates.reduceTemplate headerStr sourceTypeStr argsStr resultTypeStr resultTypeStr varsStr (varExprToStr context.CurrentVarExpr vars) exprsStr (varExprToStr context.AccVarExpr vars) "0" "+"
                        { Source = source; ReductionType = context.ReductionType; SourceArgs = [| gpuArray |]; ValueArgs = valueArgs }
                    | _ -> failwithf "Not supported %A" context.ReductionType
                | ZipWith ((Constant (first, Named (TypeCheck gpuArrayTypeDef _, [|_|])) as firstExpr), 
                            (Constant (second, Named (TypeCheck gpuArrayTypeDef _, [|_|])) as secondExpr), Lambda ([firstParamExpr; secondParamExpr], bodyExpr)) ->
                    let vars = context.VarExprs @ [firstParamExpr; secondParamExpr]
                    let resultTypeStr = typeToStr context.ResultType
                    let firstGpuArray = first :?> IGpuArray
                    let secondGpuArray = second :?> IGpuArray
                    let sourceLength = firstGpuArray.Length
                    let exprs, paramExprs, values = constantLifting context.Exprs
                    let vars = Seq.append paramExprs vars
                    let headerStr = headerStr (exprs, paramExprs, values)
                    let valueArgs = collectValueArgs (paramExprs, values) 
                    let (exprsStr, varsStr) = bodyStr exprs vars
                    let argsStr = argsToStr paramExprs vars
                    match context.ReductionType with
                    | ReductionType.Map ->
                        let source = KernelTemplates.zip2Template headerStr (typeToStr firstGpuArray.Type) (typeToStr secondGpuArray.Type) argsStr resultTypeStr 
                                                                     varsStr (varExprToStr firstParamExpr vars) (varExprToStr secondParamExpr vars) 
                                                                     (varExprToStr context.CurrentVarExpr vars) (exprToStr bodyExpr vars)
                                                                     exprsStr (varExprToStr context.AccVarExpr vars)
                        { Source = source; ReductionType = context.ReductionType; SourceArgs = [| firstGpuArray; secondGpuArray |]; ValueArgs = valueArgs  }
                    | ReductionType.Filter ->
                        let source = KernelTemplates.zip2FilterTemplate headerStr (typeToStr firstGpuArray.Type) (typeToStr secondGpuArray.Type) argsStr resultTypeStr 
                                                                            varsStr (varExprToStr firstParamExpr vars) (varExprToStr secondParamExpr vars) 
                                                                            (varExprToStr context.CurrentVarExpr vars) (exprToStr bodyExpr vars)
                                                                            exprsStr (varExprToStr context.FlagVarExpr vars) (varExprToStr context.AccVarExpr vars)
                        { Source = source; ReductionType = context.ReductionType; SourceArgs = [| firstGpuArray; secondGpuArray |]; ValueArgs = valueArgs  }
                    | ReductionType.Sum | ReductionType.Count -> 
                        let source = KernelTemplates.zip2ReduceTemplate headerStr (typeToStr firstGpuArray.Type) (typeToStr secondGpuArray.Type) argsStr resultTypeStr resultTypeStr 
                                                                        varsStr (varExprToStr firstParamExpr vars) (varExprToStr secondParamExpr vars) 
                                                                        (varExprToStr context.CurrentVarExpr vars) (exprToStr bodyExpr vars) exprsStr (varExprToStr context.AccVarExpr vars) "0" "+"
                        { Source = source; ReductionType = context.ReductionType; SourceArgs = [| firstGpuArray; secondGpuArray |]; ValueArgs = valueArgs  }
                    | _ -> failwithf "Not supported %A" context.ReductionType
                | Transform (Lambda ([paramExpr], bodyExpr), queryExpr') ->
                    let exprs' = assign context.CurrentVarExpr bodyExpr :: context.Exprs
                    compile' queryExpr' { context with CurrentVarExpr = paramExpr; VarExprs = paramExpr :: context.VarExprs; Exprs = exprs' }
                | Filter (Lambda ([paramExpr], bodyExpr), queryExpr') ->
                    match context.ReductionType with
                    | ReductionType.Map | ReductionType.Filter ->
                        let exprs' = (ifThenElse bodyExpr (block [] [assign context.FlagVarExpr (constant 0)]) (block [] [assign context.FlagVarExpr (constant 1); (``continue`` context.ContinueLabel)])) :: assign context.CurrentVarExpr paramExpr :: context.Exprs
                        compile' queryExpr' { context with CurrentVarExpr = paramExpr; VarExprs = paramExpr :: context.VarExprs; Exprs = exprs'; ReductionType = ReductionType.Filter } 
                    | _ ->
                        let exprs' = (ifThenElse bodyExpr (block [] [empty]) (block [] [``continue`` context.ContinueLabel])) :: assign context.CurrentVarExpr paramExpr :: context.Exprs
                        compile' queryExpr' { context with CurrentVarExpr = paramExpr; VarExprs = paramExpr :: context.VarExprs; Exprs = exprs' } 
                | _ -> failwithf "Not supported %A" queryExpr 

            let finalVarExpr = var "___final___" queryExpr.Type
            let flagVarExpr = var "___flag___" typeof<int>
            match queryExpr with
            | Transform (_) ->
                let context = { CurrentVarExpr = finalVarExpr; AccVarExpr = finalVarExpr; FlagVarExpr = flagVarExpr;
                                BreakLabel = breakLabel (); ContinueLabel = continueLabel (); 
                                InitExprs = []; AccExpr = empty; CombinerExpr = empty; ResultType = queryExpr.Type; 
                                VarExprs = [finalVarExpr; flagVarExpr]; Exprs = []; ReductionType = ReductionType.Map  }
                compile' queryExpr context
            | Filter (_) ->
                let context = { CurrentVarExpr = finalVarExpr; AccVarExpr = finalVarExpr; FlagVarExpr = flagVarExpr;
                                BreakLabel = breakLabel (); ContinueLabel = continueLabel (); 
                                InitExprs = []; AccExpr = empty; CombinerExpr = empty; ResultType = queryExpr.Type; 
                                VarExprs = [finalVarExpr; flagVarExpr]; Exprs = []; ReductionType = ReductionType.Filter }
                compile' queryExpr context
            | Sum (queryExpr') ->
                let context = { CurrentVarExpr = finalVarExpr; AccVarExpr = finalVarExpr; FlagVarExpr = flagVarExpr;
                                BreakLabel = breakLabel (); ContinueLabel = continueLabel (); 
                                InitExprs = []; AccExpr = empty; CombinerExpr = empty; ResultType = queryExpr.Type; 
                                VarExprs = [finalVarExpr; flagVarExpr]; Exprs = []; ReductionType = ReductionType.Sum }
                compile' queryExpr' context
            | Count (queryExpr') ->
                let context = { CurrentVarExpr = finalVarExpr; AccVarExpr = finalVarExpr; FlagVarExpr = flagVarExpr;
                                BreakLabel = breakLabel (); ContinueLabel = continueLabel (); 
                                InitExprs = []; AccExpr = empty; CombinerExpr = empty; ResultType = queryExpr.Type; 
                                VarExprs = [finalVarExpr; flagVarExpr]; Exprs = []; ReductionType = ReductionType.Count }
                compile' (Transform ((lambda [|var "___empty___" queryExpr'.Type|] (constant 1)),queryExpr')) context
            | ToArray (queryExpr') -> compile queryExpr'
            | ZipWith (_, _, _) ->
                let context = { CurrentVarExpr = finalVarExpr; AccVarExpr = finalVarExpr; FlagVarExpr = flagVarExpr;
                                BreakLabel = breakLabel (); ContinueLabel = continueLabel (); 
                                InitExprs = []; AccExpr = empty; CombinerExpr = empty; ResultType = queryExpr.Type; 
                                VarExprs = [finalVarExpr; flagVarExpr]; Exprs = []; ReductionType = ReductionType.Map  }
                compile' queryExpr context
            | _ -> failwithf "Not supported %A" queryExpr 

