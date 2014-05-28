namespace Nessos.GpuLinq.Core
    
    open System
    open System.Collections
    open System.Collections.Generic
    open System.Linq
    open System.Linq.Expressions
    open System.Reflection
    open System.Collections.Concurrent
    open Nessos.LinqOptimizer.Core

    /// Returns the free variables out of an expression.
    type private FreeVariablesVisitor () =
        inherit ExpressionVisitor() with

            let (|GpuFunctionInvoke|_|) (expr : Expression) =
                match expr with
                | MethodCall (objExpr, methodInfo, (Parameter funcExpr :: args)) 
                    when typeof<Expression>.IsAssignableFrom(funcExpr.Type) 
                    && methodInfo.Name = "Invoke" ->
                    Some(funcExpr, args)
                | _ -> None

            let freeVars  = HashSet<ParameterExpression>()
            let localVars = HashSet<ParameterExpression>()

            let addLocals(vars : seq<ParameterExpression>) =
                vars |> Seq.iter (fun p -> localVars.Add(p) |> ignore)

            member this.Environment with get () = freeVars :> seq<_>
            member this.Locals with get() = localVars :> seq<_>

            override this.VisitParameter(expr : ParameterExpression) =
                if not <| localVars.Contains(expr) then
                    freeVars.Add(expr) |> ignore
                expr :> _
                
            override this.VisitLambda(expr : Expression<'T>) =
                addLocals expr.Parameters
                expr.Update(this.Visit(expr.Body), expr.Parameters) :> _

            override this.VisitBlock(expr : BlockExpression) =
                addLocals expr.Variables 
                expr.Update(expr.Variables, this.Visit(expr.Expressions)) :> _

            override this.VisitMethodCall(expr : MethodCallExpression) =
                match expr with
                | GpuFunctionInvoke(fExpr, args) -> 
                    let vs = this.Visit(ObjectModel.ReadOnlyCollection(args.ToArray()))
                    let args = (fExpr :> Expression) :: (vs |> Seq.toList)
                    expr.Update(this.Visit(expr.Object), args) :> _
                | _ -> expr.Update(this.Visit(expr.Object), this.Visit(expr.Arguments)) :> _

            override this.VisitMember(expr : MemberExpression) =
                // TransparentIdentifier's free variable
                if expr.Member.MemberType = MemberTypes.Property then
                    let pi = expr.Member :?> PropertyInfo
                    if isTransparentIdentifier expr.Expression then
                        let p = Expression.Parameter(expr.Type, pi.Name)
                        freeVars.Add(p) |> ignore
                        p :> _
                    else
                        expr :> _
                else
                    expr :> _

//            override this.VisitMember(expr : MemberExpression) =
//                if expr.Expression :? ConstantExpression then
//                    let obj = (expr.Expression :?> ConstantExpression).Value
//                    
//                    let (value, p) = 
//                        match expr.Member.MemberType with
//                        | MemberTypes.Field ->
//                            let fi = expr.Member :?> FieldInfo
//                            fi.GetValue(obj), Expression.Parameter(expr.Type, sprintf "%s" fi.Name) 
//                        | MemberTypes.Property ->
//                            let pi = expr.Member :?> PropertyInfo
//                            let indexed = pi.GetIndexParameters() |> Seq.cast<obj> |> Seq.toArray
//                            pi.GetValue(obj, indexed), Expression.Parameter(expr.Type, sprintf "%s" pi.Name) 
//                        | _ -> 
//                            failwithf "Internal error : Accessing non Field or Property from MemberExpression %A" expr
//                    freeVars.Add(p) |> ignore
//                    p :> _
//                else
//                    expr.Update(this.Visit expr.Expression) :> _


    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    module FreeVariablesVisitor =
        let get(expr : Expression) =
            let fvv = new FreeVariablesVisitor()
            let expr = fvv.Visit(expr)
            fvv.Environment

        let getLocals(expr : Expression) =
            let fvv = new FreeVariablesVisitor()
            let expr = fvv.Visit(expr)
            fvv.Locals

        let getWithExpr(expr : Expression) =
            let fvv = new FreeVariablesVisitor()
            let expr' = fvv.Visit(expr)
            expr', fvv.Environment