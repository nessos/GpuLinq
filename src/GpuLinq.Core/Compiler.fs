namespace Nessos.GpuLinq.Core
    open System
    open System.Linq
    open System.Linq.Expressions
    open System.Runtime.InteropServices
    open OpenCL.Net
    open Nessos.LinqOptimizer.Core
    open Nessos.LinqOptimizer.Core.Utils

    module Compiler =
        type Length = int
        type Size = int

        type QueryContext = { CurrentVarExpr : ParameterExpression; AccVarExpr : ParameterExpression; FlagVarExpr : ParameterExpression;
                                BreakLabel : LabelTarget; ContinueLabel : LabelTarget;
                                InitExprs : Expression list; AccExpr : Expression; CombinerExpr : Expression; ResultType : Type; 
                                VarExprs : ParameterExpression list; Exprs : Expression list; ReductionType : ReductionType }
        
        type SourceType = SingleSource | NestedSource | Zip
        type CompilerResult = { Source : string; SourceType : SourceType; ReductionType : ReductionType; SourceArgs : (ParameterExpression * IGpuArray) []; ValueArgs : (ParameterExpression * obj) [] }
        
        let intType = typeof<int>
        let longType = typeof<int64>
        let floatType = typeof<single>
        let doubleType = typeof<double>
        let byteType = typeof<byte>
        let boolType = typeof<bool>
        let gpuArrayTypeDef = typedefof<GpuArray<_>>
        let igpuArrayTypeDef = typedefof<IGpuArray<_>>

        let fieldToType (fieldInfo : System.Reflection.FieldInfo) = fieldInfo.FieldType
             

        let breakLabel () = labelTarget "brk"
        let continueLabel () = labelTarget "cont"

        let (|GpuFunctionInvoke|_|) (expr : Expression) =
            match expr with
            | MethodCall (objExpr, methodInfo, (Parameter funcExpr :: args)) 
                when typeof<Expression>.IsAssignableFrom(funcExpr.Type) 
                && methodInfo.Name = "Invoke" ->
                Some(funcExpr, args)
            | _ -> None
                        
        let rec toQueryExpr (expr : Expression) : QueryExpr = 
            match expr with 
            | MethodCall (_, MethodName "Select" _, [expr'; LambdaOrQuote ([_], bodyExpr, f')]) -> 
                Transform (f', toQueryExpr expr')    
            | MethodCall (_, MethodName "Where" _, [expr'; LambdaOrQuote ([paramExpr], _, f')]) -> 
                Filter (f', toQueryExpr expr')
            | MethodCall (_, MethodName "Aggregate" _, [expr'; seedExpr; LambdaOrQuote ([_;_] as paramsExpr, bodyExpr, f') ] ) ->
                Aggregate(seedExpr, f' , toQueryExpr expr')    
            | MethodCall (_, (MethodName "SelectMany" _), [expr'; LambdaOrQuote ([paramExpr], bodyExpr, _); LambdaOrQuote (_,_,lam)]) -> 
                NestedQueryTransform((paramExpr, toQueryExpr (bodyExpr)), lam, toQueryExpr expr')
            | MethodCall (_, MethodName "Count" _,  [expr']) -> 
                Count(toQueryExpr expr')
            | MethodCall (_, MethodName "Sum" _,  [expr']) -> 
                Sum(toQueryExpr expr')
            | MethodCall (_, MethodName "ToArray" _, [expr']) ->
                ToArray(toQueryExpr expr')
            | MethodCall (_, MethodName "Zip" _, [expr; expr'; LambdaOrQuote ([_; _], bodyExpr, f')] ) ->
                ZipWith (expr, expr, f')
            | MethodCall (_, MethodName "AsGpuQueryExpr" _, [expr']) ->
                match expr'.Type with
                | Named(typedef, [|elemType|]) when typedef = typedefof<IGpuArray<_>> -> 
                    Source (expr', elemType, QueryExprType.Gpu)
                | _ -> failwithf "Not supported %A" expr'.Type
            | _ -> failwithf "Not supported %A" expr

        let rec compile (queryExpr : QueryExpr) : CompilerResult = 

            let rec compile' (queryExpr : QueryExpr) (context : QueryContext) =
                let rec typeToStr (t : Type) = 
                    match t with
                    | TypeCheck intType _ -> "int"
                    | TypeCheck longType _ -> "long"
                    | TypeCheck floatType _ -> "float"
                    | TypeCheck doubleType _ -> "double"
                    | TypeCheck byteType _ -> "byte"
                    | Named(typedef, [|elemType|]) when typedef = typedefof<IGpuArray<_>> -> 
                        sprintf' "__global %s*" (typeToStr elemType)
                    | _ when t.IsValueType && not t.IsPrimitive -> t.Name                    
                    | _ -> failwithf "Not supported %A" t

                let varExprToStr (varExpr : ParameterExpression) (vars : seq<ParameterExpression>) = 
                    match vars |> List.ofSeq |> List.filter (fun varExpr' -> varExpr.Name = varExpr'.Name) with
                    | [] -> varExpr.Name
                    | vars' -> 
                        let index = vars' |> List.findIndex (fun varExpr' -> varExpr = varExpr')
                        sprintf' "___%s___%d" (varExpr.ToString()) index
                    
                let varsToStr (vars : seq<ParameterExpression>) = 
                    let varsStr = 
                        vars
                        |> Seq.filter (fun varExpr -> not (varExpr.Name.StartsWith("___param")))
                        |> Seq.filter (fun varExpr -> not (isAnonymousType varExpr.Type))
                        |> Seq.map (fun varExpr -> sprintf' "%s %s;" (typeToStr varExpr.Type) (varExprToStr varExpr vars)) 
                        |> Seq.fold (fun first second -> sprintf' "%s%s%s" first Environment.NewLine second) "" 
                    varsStr
                let argsLifting (vars : seq<ParameterExpression>) (exprs : Expression list) = 
                    match exprs with
                    | [] -> ([||], [||], [||])
                    | _ ->
                        let expr, paramExprs, objs = ArgsCollector.apply vars (block [] exprs) 
                        ((expr :?> BlockExpression).Expressions.ToArray(), paramExprs, objs)
                let argLifting (vars : seq<ParameterExpression>) (expr : Expression) = 
                    let expr, paramExprs, objs = ArgsCollector.apply vars (block [] [expr]) 
                    ((expr :?> BlockExpression).Expressions.ToArray(), (paramExprs |> Seq.head), (objs |> Seq.head))
                let isCustomStruct (t : Type) = t.IsValueType && not t.IsPrimitive
                let structToStr (t : Type) = 
                    let fields = t.GetFields().OrderBy(fun f -> Marshal.OffsetOf(t, f.Name).ToInt32()).ToArray()
                    let fieldsStr = 
                        (fields, "")
                        ||> Array.foldBack (fun fieldInfo fieldsStr -> sprintf' "%s %s; %s" (typeToStr fieldInfo.FieldType) fieldInfo.Name fieldsStr) 
                    sprintf' "typedef struct { %s } %s;" fieldsStr t.Name
                let customStructsToStr (types : seq<Type>) = 
                    types 
                    |> Seq.map structToStr
                    |> Seq.fold (fun first second -> sprintf' "%s%s%s" first Environment.NewLine second) ""
                let structsDefinitionStr (types : seq<Type>) =
                    let customTypes = 
                        types
                        |> Seq.filter isCustomStruct 
                        |> Seq.distinctBy (fun t -> t.FullName)
                        |> Array.ofSeq
                    customStructsToStr customTypes
                let argsToStr (argParamExprs : ParameterExpression[]) (paramExprs : seq<ParameterExpression>) = 
                    let argParamExprs = argParamExprs |> Array.filter (fun paramExpr -> not <| typeof<Expression>.IsAssignableFrom(paramExpr.Type))
                    (argParamExprs, "") 
                    ||> Array.foldBack (fun paramExpr result -> sprintf' "%s %s, %s" (typeToStr paramExpr.Type) (varExprToStr paramExpr paramExprs) result) 
                let rec isValidQueryExpr (expr : Expression) = 
                    match expr with
                    | MethodCall (_, MethodName "Select" _, [expr'; LambdaOrQuote ([_], bodyExpr, f')]) -> 
                        isValidQueryExpr expr'
                    | MethodCall (_, MethodName "Where" _, [expr'; LambdaOrQuote ([paramExpr], _, f')]) -> 
                        isValidQueryExpr expr'
                    | MethodCall (_, MethodName "Take" _, [expr'; countExpr]) when countExpr.Type = typeof<int> -> 
                        isValidQueryExpr expr'
                    | MethodCall (_, MethodName "Skip" _, [expr'; countExpr]) when countExpr.Type = typeof<int> -> 
                        isValidQueryExpr expr'
                    | MethodCall (_, MethodName "Count" _, [expr']) -> 
                        isValidQueryExpr expr'
                    | MethodCall (_, MethodName "Generate" _, [startExpr; Lambda (_,_) as pred; LambdaOrQuote(_,_,state); LambdaOrQuote(_,_,result)]) ->
                        true
                    | _ -> false

                let rec exprToStr (expr : Expression) (vars : seq<ParameterExpression>) =
                    match expr with
                    // AnonymousType handling
                    | AnonymousTypeMember expr ->
                        match vars |> Seq.tryFind (fun varExpr -> varExpr.Name = expr.Member.Name) with
                        | Some varExpr -> varExprToStr varExpr vars
                        | None -> expr.Member.Name
                    | AnonymousTypeAssign (_, AnonymousTypeConstruction (members, args)) ->
                        sprintf' "%s %s = %s" (typeToStr <| args.Last().Type) (members.Last().Name) (exprToStr <| args.Last() <| vars)
                    | FieldMember (expr, fieldMember) -> sprintf' "%s.%s" (exprToStr expr vars) fieldMember.Name
                    | MethodCall (objExpr, methodInfo, [argExpr]) when methodInfo.Name = "get_Item" ->
                        sprintf' "%s[%s]" (exprToStr objExpr vars) (exprToStr argExpr vars)
                    // Math functions
                    | PropertyMember(expr, mi) when mi.Name = "PI" ->
                        let pi = mi :?> System.Reflection.PropertyInfo
                        let value = pi.GetValue(null, null)
                        sprintf' "%A" value
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
                    // Inner Enumerable Methods
                    | MethodCall (_, MethodName "Count" _,  [expr']) 
                    | MethodCall (_, MethodName "Sum" _,  [expr']) when isValidQueryExpr expr'->
                        let freeVars = Nessos.GpuLinq.Core.FreeVariablesVisitor.get expr
                        let argsStr = 
                            freeVars
                            |> Seq.map (fun paramExpr -> varExprToStr paramExpr vars)
                            |> String.concat ", "
                        sprintf' "(___func___%d(%s))" (Math.Abs(expr.ToString().GetHashCode()))  argsStr
                    // Expr Call
                    | GpuFunctionInvoke(funcExpr, args) when args.Length >= 1 ->
                        let paramsString =
                            args |> List.map (fun arg -> exprToStr arg vars)
                                 |> List.reduce (fun l r -> sprintf' "%s, %s" l r)
                        sprintf' "%s(%s)" funcExpr.Name paramsString
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
                    | AddAssign (Parameter (paramExpr), expr') -> sprintf' "%s += %s" (varExprToStr paramExpr vars) (exprToStr expr' vars)
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
                        match thenExpr.NodeType, elseExpr.NodeType with
                        | (ExpressionType.Block | ExpressionType.Goto | ExpressionType.Default), (ExpressionType.Block | ExpressionType.Goto | ExpressionType.Default) ->
                            sprintf' "if (%s) 
                                      { 
                                          %s; 
                                      } 
                                      else 
                                      { 
                                          %s; 
                                      }" (exprToStr testExpr vars) (exprToStr thenExpr vars) (exprToStr elseExpr vars)
                        | _ ->
                            sprintf' "((%s) ? %s : %s)" (exprToStr testExpr vars) (exprToStr thenExpr vars) (exprToStr elseExpr vars)
                    | Goto (kind, target, value) when kind = GotoExpressionKind.Goto -> 
                        sprintf' "goto %s" target.Name 
                    | Goto (kind, target, value) when kind = GotoExpressionKind.Continue -> 
                        "continue" 
                    | Goto (kind, target, value) when kind = GotoExpressionKind.Break -> 
                        "break" 
                    | Block (paramExprs, exprs, resultExpr) -> 
                        let vars = Seq.append vars paramExprs
                        let varsStr = varsToStr paramExprs
                        let exprsStr = 
                            exprs
                                |> Seq.map (fun expr -> sprintf' "%s" (exprToStr expr vars))
                                |> Seq.reduce (fun first second -> sprintf' "%s;
                                                                             %s" first second)
                        sprintf' "%s
                                  %s" varsStr exprsStr
                    | Loop (bodyExpr, breakLabel, continueLabel) ->
                        sprintf' "while(true) 
                                  {
                                    %s;
                                  }" (exprToStr bodyExpr vars)
                    | Convert (expr, t) -> sprintf' "((%s) %s)" (typeToStr t) (exprToStr expr vars)
                    | Nop _ -> ""
                    | _ -> failwithf "Not supported %A" expr

                let rec collectFuncsStr (paramExprs : ParameterExpression[], values : obj[]) = 
                    
                    let rec collect_aux (paramExprs : ParameterExpression[], values : obj[]) =
                        let funcsStr = 
                            (paramExprs, values) 
                            ||> Array.zip 
                            |> Array.map (fun (paramExpr, value) -> 
                                            match value with
                                            | :? Expression as expr -> 
                                                match expr with
                                                | Lambda (varExprs, bodyExpr) -> 
                                                    let expr, paramExprs, objs as h = ConstantLiftingTransformer.apply bodyExpr
                                                    Seq.append [paramExpr, varExprs, h] (collect_aux(paramExprs, objs))
                                                | _ -> [] :> _
                                            | _ -> [] :> _)
                        
                        funcsStr |> Seq.concat

                    let funcs = collect_aux(paramExprs, values)
                                |> Seq.groupBy (fun (param,a,b) -> param.Name) // 
                                |> Seq.map (snd >> Seq.head)                   // Distinct
                                |> Seq.toArray

                    let ordered = GpuFunctionDependencyAnalysis.sort(funcs)
                    let funcsStr = ordered
                                   |> Array.map(fun (paramExpr, varExprs, (expr, paramExprs, objs)) ->

                                                    match expr with
                                                    | Block (blockVars, exprs, Parameter resultParamExpr) -> 
                                                        let parameterStr = varExprs 
                                                                           |> Seq.map (fun varExpr -> 
                                                                                sprintf' "%s %s" (typeToStr varExpr.Type) (varExprToStr varExpr varExprs))
                                                                           |> Seq.reduce (sprintf' "%s, %s")
                                                        let varExprs = Seq.append varExprs blockVars 
                                                        let exprStr = exprToStr expr varExprs
                                                        let funcStr = sprintf' "inline %s %s(%s) 
                                                                                { 
                                                                                    %s; 
                                                                                    return %s;
                                                                                }%s" 
                                                                        (typeToStr expr.Type) paramExpr.Name parameterStr exprStr 
                                                                        (varExprToStr resultParamExpr varExprs) Environment.NewLine
                                                        funcStr
                                                    | _ ->                                                     
                                                        let exprStr = exprToStr expr varExprs
                                                        let parameterStr = varExprs 
                                                                           |> Seq.map (fun varExpr -> 
                                                                                sprintf' "%s %s" (typeToStr varExpr.Type) (varExprToStr varExpr varExprs))
                                                                           |> Seq.reduce (sprintf' "%s, %s")
                                                        let funcStr = sprintf' "inline %s %s(%s) { return %s; }%s " 
                                                                        (typeToStr expr.Type) paramExpr.Name parameterStr exprStr Environment.NewLine
                                                        funcStr)
                    let collect = funcsStr |> String.concat Environment.NewLine
                    collect

                let headerStr (types : seq<Type>, paramExprs : ParameterExpression[], values : obj[]) = 
                    let funcsStr = collectFuncsStr (paramExprs, values) 
                    let structsStr = structsDefinitionStr types
                    [KernelTemplates.openCLExtensions; structsStr; funcsStr] |> String.concat Environment.NewLine

                let bodyStr (exprs : seq<Expression>) (vars : seq<ParameterExpression>) =
                    let exprsStr = exprs
                                       |> Seq.map (fun expr -> sprintf' "%s;" (exprToStr expr vars))
                                       |> Seq.fold (fun first second -> sprintf' "%s%s%s" first Environment.NewLine second) ""
                    let varsStr = varsToStr vars
                    (exprsStr, varsStr)

                let collectValueArgs (paramExprs : ParameterExpression[], values : obj[]) = 
                    (paramExprs, values) 
                    ||> Array.zip 
                    |> Array.filter (fun (paramExpr, value) -> not (typeof<Expression>.IsAssignableFrom(paramExpr.Type)))
                    |> Array.map (fun (paramExpr, value) -> (paramExpr, value))

                match queryExpr with
                | Source (expr, sourceType, QueryExprType.Gpu) ->
                    let sourceTypeStr = typeToStr sourceType
                    let resultTypeStr = typeToStr context.ResultType
                    let (gpuArrayParamExpr, gpuArray) =
                        match expr with
                        | Constant (value, Named (TypeCheck gpuArrayTypeDef _, [|_|])) -> (var "___input___" <| value.GetType()), value :?> IGpuArray 
                        | FieldMember (_, _) -> 
                            let _, gpuArrayParamExpr, gpuArray = argLifting context.VarExprs expr
                            (gpuArrayParamExpr, gpuArray :?> IGpuArray)
                        | Parameter gpuArrayParamExpr -> (gpuArrayParamExpr, Unchecked.defaultof<IGpuArray>)
                        | _ -> failwithf "Not supported %A" expr
                    let exprs, paramExprs, values = argsLifting context.VarExprs context.Exprs
                    let vars = context.VarExprs
                    let paramExprs', values'  = QuerySubExpression.get isValidQueryExpr exprs 
                    let paramExprs, values = (Array.append paramExprs paramExprs'), (Array.append values values')
                    let headerStr = headerStr (TypeCollector.getTypes exprs, paramExprs, values)
                    let valueArgs = collectValueArgs (paramExprs, values) 
                    let (exprsStr, varsStr) = bodyStr exprs vars
                    let argsStr = argsToStr paramExprs vars 
                    match context.ReductionType with
                    | ReductionType.Map ->
                        let source = KernelTemplates.mapTemplate headerStr sourceTypeStr argsStr resultTypeStr varsStr (varExprToStr context.CurrentVarExpr vars) exprsStr (varExprToStr context.AccVarExpr vars)
                        { Source = source; SourceType = SourceType.SingleSource; ReductionType = context.ReductionType; SourceArgs = [| (gpuArrayParamExpr, gpuArray) |]; ValueArgs = valueArgs  }
                    | ReductionType.Filter ->
                        let source = KernelTemplates.mapFilterTemplate headerStr sourceTypeStr argsStr resultTypeStr varsStr (varExprToStr context.CurrentVarExpr vars) exprsStr (varExprToStr context.FlagVarExpr vars) (varExprToStr context.AccVarExpr vars) 
                        { Source = source; SourceType = SourceType.SingleSource; ReductionType = context.ReductionType; SourceArgs = [| (gpuArrayParamExpr, gpuArray) |]; ValueArgs = valueArgs }
                    | ReductionType.Sum | ReductionType.Count -> 
                        
                        let source = KernelTemplates.reduceTemplate headerStr sourceTypeStr argsStr resultTypeStr resultTypeStr varsStr (varExprToStr context.CurrentVarExpr vars) exprsStr (varExprToStr context.AccVarExpr vars) "0" "+"
                        { Source = source; SourceType = SourceType.SingleSource; ReductionType = context.ReductionType; SourceArgs = [| (gpuArrayParamExpr, gpuArray) |]; ValueArgs = valueArgs }
                    | _ -> failwithf "Not supported %A" context.ReductionType
                | NestedQueryTransform ((_, Source (FieldMember (_, (Map fieldToType (Named (TypeCheck igpuArrayTypeDef _, [|_|])) as fieldInfo)) as nestedExpr, nestedSourceType, QueryExprType.Gpu)), projectLambdaExpr, 
                                            Source (expr, sourceType, QueryExprType.Gpu)) ->
                    let exprs, paramExprs, values = argsLifting context.VarExprs context.Exprs
                    let vars = Seq.append paramExprs context.VarExprs
                    // Extract Nested GpuArray
                    let _, nestedGpuArrayParamExpr, nestedGpuArray = argLifting vars nestedExpr
                    // EXtact projections
                    let firstParamExpr, secondParamExpr, projectExpr = 
                        match projectLambdaExpr with
                        | Lambda ([firstParamExpr; secondParamExpr], AnonymousTypeConstruction (members, args)) -> 
                            firstParamExpr, secondParamExpr, empty :> Expression
                        | Lambda ([firstParamExpr; secondParamExpr], bodyExpr) -> firstParamExpr, secondParamExpr, bodyExpr
                        | _ -> failwithf "Invalid state %A" projectLambdaExpr 
                    let vars = Seq.append [|firstParamExpr; secondParamExpr|] vars
                    let paramExprs', values'  = QuerySubExpression.get isValidQueryExpr exprs
                    let paramExprs, values = (Array.append paramExprs paramExprs'), (Array.append values values')
                    let sourceTypeStr = typeToStr sourceType
                    let nestedSourceTypeStr = typeToStr nestedSourceType
                    let resultTypeStr = typeToStr context.ResultType
                    let (gpuArrayParamExpr, gpuArray) =
                        match expr with
                        | Constant (value, Named (TypeCheck gpuArrayTypeDef _, [|_|])) -> (var "___input___" <| value.GetType()), value :?> IGpuArray 
                        | FieldMember (_, _) -> 
                            let _, gpuArrayParamExpr, gpuArray = argLifting context.VarExprs expr
                            (gpuArrayParamExpr, gpuArray :?> IGpuArray)
                        | Parameter gpuArrayParamExpr -> (gpuArrayParamExpr, Unchecked.defaultof<IGpuArray>)
                        | _ -> failwithf "Not supported %A" expr
                    let headerStr = headerStr (TypeCollector.getTypes exprs, (paramExprs), values)
                    let valueArgs = collectValueArgs (paramExprs, values) 
                    let (exprsStr, varsStr) = bodyStr exprs vars
                    let argsStr = argsToStr paramExprs vars
                    
                    match context.ReductionType with
                    | ReductionType.NestedQueryTransform ->
                        let source = KernelTemplates.nestedMapTemplate headerStr sourceTypeStr nestedSourceTypeStr argsStr resultTypeStr 
                                        varsStr (varExprToStr firstParamExpr vars) (varExprToStr secondParamExpr vars) exprsStr  (exprToStr projectExpr vars)
                        { Source = source; SourceType = SourceType.NestedSource; ReductionType = context.ReductionType; SourceArgs = [| (gpuArrayParamExpr, gpuArray); (nestedGpuArrayParamExpr, (nestedGpuArray :?> IGpuArray))|]; ValueArgs = valueArgs }
                    | ReductionType.Map -> 
                        let source = KernelTemplates.nestedMapTemplate headerStr sourceTypeStr nestedSourceTypeStr argsStr resultTypeStr 
                                        varsStr (varExprToStr firstParamExpr vars) (varExprToStr secondParamExpr vars) exprsStr (varExprToStr context.AccVarExpr vars)
                        { Source = source; SourceType = SourceType.NestedSource; ReductionType = context.ReductionType; SourceArgs = [| (gpuArrayParamExpr, gpuArray); (nestedGpuArrayParamExpr, (nestedGpuArray :?> IGpuArray)) |]; ValueArgs = valueArgs }
                    | _ -> failwithf "Not supported %A" context.ReductionType

                | ZipWith (firstExpr, secondExpr, Lambda ([firstParamExpr; secondParamExpr], bodyExpr)) ->
                    let vars = context.VarExprs @ [firstParamExpr; secondParamExpr]
                    let resultTypeStr = typeToStr context.ResultType
                    let (firstGpuArrayParamExpr, firstGpuArray) =
                        match firstExpr with
                        | Constant (value, Named (TypeCheck gpuArrayTypeDef _, [|_|])) -> (var "___input___" <| value.GetType()), value :?> IGpuArray 
                        | FieldMember (_, _) -> 
                            let _, gpuArrayParamExpr, gpuArray = argLifting context.VarExprs firstExpr
                            (gpuArrayParamExpr, gpuArray :?> IGpuArray)
                        | Parameter gpuArrayParamExpr -> (gpuArrayParamExpr, Unchecked.defaultof<IGpuArray>)
                        | _ -> failwithf "Not supported %A" firstExpr
                    let (secondGpuArrayParamExpr, secondGpuArray) =
                        match secondExpr with
                        | Constant (value, Named (TypeCheck gpuArrayTypeDef _, [|_|])) -> (var "___input___" <| value.GetType()), value :?> IGpuArray 
                        | FieldMember (_, _) -> 
                            let _, gpuArrayParamExpr, gpuArray = argLifting context.VarExprs secondExpr
                            (gpuArrayParamExpr, gpuArray :?> IGpuArray)
                        | Parameter gpuArrayParamExpr -> (gpuArrayParamExpr, Unchecked.defaultof<IGpuArray>)
                        | _ -> failwithf "Not supported %A" secondExpr
                    let exprs, paramExprs, values = argsLifting vars context.Exprs
                    let vars = Seq.append paramExprs vars
                    let headerStr = headerStr (TypeCollector.getTypes context.Exprs, paramExprs, values)
                    let valueArgs = collectValueArgs (paramExprs, values) 
                    let (exprsStr, varsStr) = bodyStr exprs vars
                    let argsStr = argsToStr paramExprs vars
                    let firstGpuArrayType = firstGpuArrayParamExpr.Type.GetGenericArguments().[0]
                    let secondGpuArrayType = secondGpuArrayParamExpr.Type.GetGenericArguments().[0]
                    match context.ReductionType with
                    | ReductionType.Map ->
                        let source = KernelTemplates.zip2Template headerStr (typeToStr firstGpuArrayType) (typeToStr secondGpuArrayType) argsStr resultTypeStr 
                                                                     varsStr (varExprToStr firstParamExpr vars) (varExprToStr secondParamExpr vars) 
                                                                     (varExprToStr context.CurrentVarExpr vars) (exprToStr bodyExpr vars)
                                                                     exprsStr (varExprToStr context.AccVarExpr vars)
                        { Source = source; SourceType = SourceType.Zip; ReductionType = context.ReductionType; SourceArgs = [| (firstGpuArrayParamExpr, firstGpuArray); (secondGpuArrayParamExpr, secondGpuArray) |]; ValueArgs = valueArgs  }
                    | ReductionType.Filter ->
                        let source = KernelTemplates.zip2FilterTemplate headerStr (typeToStr firstGpuArrayType) (typeToStr secondGpuArrayType) argsStr resultTypeStr 
                                                                            varsStr (varExprToStr firstParamExpr vars) (varExprToStr secondParamExpr vars) 
                                                                            (varExprToStr context.CurrentVarExpr vars) (exprToStr bodyExpr vars)
                                                                            exprsStr (varExprToStr context.FlagVarExpr vars) (varExprToStr context.AccVarExpr vars)
                        { Source = source; SourceType = SourceType.Zip; ReductionType = context.ReductionType; SourceArgs = [| (firstGpuArrayParamExpr, firstGpuArray); (secondGpuArrayParamExpr, secondGpuArray) |]; ValueArgs = valueArgs  }
                    | ReductionType.Sum | ReductionType.Count -> 
                        let source = KernelTemplates.zip2ReduceTemplate headerStr (typeToStr firstGpuArrayType) (typeToStr secondGpuArrayType) argsStr resultTypeStr resultTypeStr 
                                                                        varsStr (varExprToStr firstParamExpr vars) (varExprToStr secondParamExpr vars) 
                                                                        (varExprToStr context.CurrentVarExpr vars) (exprToStr bodyExpr vars) exprsStr (varExprToStr context.AccVarExpr vars) "0" "+"
                        { Source = source; SourceType = SourceType.Zip; ReductionType = context.ReductionType; SourceArgs = [| (firstGpuArrayParamExpr, firstGpuArray); (secondGpuArrayParamExpr, secondGpuArray) |]; ValueArgs = valueArgs  }
                    | _ -> failwithf "Not supported %A" context.ReductionType
                | Transform (Lambda ([paramExpr], bodyExpr), queryExpr') ->
                    let exprs' = assign context.CurrentVarExpr bodyExpr :: context.Exprs
                    compile' queryExpr' { context with CurrentVarExpr = paramExpr; VarExprs = paramExpr :: context.VarExprs; Exprs = exprs' }
                | Filter (Lambda ([paramExpr], bodyExpr), queryExpr') ->
                    match context.ReductionType with
                    | ReductionType.Map | ReductionType.Filter ->
                        let exprs' = (ifThenElse bodyExpr (block [] [assign context.FlagVarExpr (constant 0)]) (block [] [assign context.FlagVarExpr (constant 1); (goto context.ContinueLabel)])) :: assign context.CurrentVarExpr paramExpr :: context.Exprs
                        compile' queryExpr' { context with CurrentVarExpr = paramExpr; VarExprs = paramExpr :: context.VarExprs; Exprs = exprs'; ReductionType = ReductionType.Filter } 
                    | _ ->
                        let exprs' = (ifThenElse bodyExpr (block [] [empty]) (block [] [goto context.ContinueLabel])) :: assign context.CurrentVarExpr paramExpr :: context.Exprs
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
            | NestedQueryTransform (_) -> 
                let context = { CurrentVarExpr = finalVarExpr; AccVarExpr = finalVarExpr; FlagVarExpr = flagVarExpr;
                                BreakLabel = breakLabel (); ContinueLabel = continueLabel (); 
                                InitExprs = []; AccExpr = empty; CombinerExpr = empty; ResultType = queryExpr.Type; 
                                VarExprs = [finalVarExpr; flagVarExpr]; Exprs = []; ReductionType = ReductionType.NestedQueryTransform  }
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

