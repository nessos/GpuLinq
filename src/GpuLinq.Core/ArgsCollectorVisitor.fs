namespace Nessos.GpuLinq.Core
    
    open System
    open System.Collections
    open System.Collections.Generic
    open System.Linq
    open System.Linq.Expressions
    open System.Reflection
    open System.Collections.Concurrent
    open Nessos.LinqOptimizer.Core

    // Lift constants and member access into parameters
    // due to the live-object limitation.
    type private ArgsCollectorVisitor (vars : seq<ParameterExpression>, localVars : seq<ParameterExpression>) =
        inherit ExpressionVisitor() with
            let mutable x = 0
            let getName () = 
                x <- x + 1
                sprintf "___param%d___" x

            let environment = Dictionary<string, ParameterExpression * obj>()

            member this.Environment with get () = environment

            override this.VisitParameter(expr : ParameterExpression) =
                if not <| (vars |> Seq.exists (fun var -> var = expr)) &&
                   not <| (localVars |> Seq.exists (fun var -> var = expr)) then
                    if not <| this.Environment.ContainsKey(expr.Name) then
                        this.Environment.Add(expr.Name, (expr, null :> obj))
                expr :> _

            override this.VisitConstant(expr : ConstantExpression) =
                if not <| isPrimitive expr && expr.Value <> null then
                    let p = Expression.Parameter(expr.Type, getName()) 
                    this.Environment.Add(p.Name, (p, expr.Value))
                    p :> _
                else 
                    expr :> _

            override this.VisitMember(expr : MemberExpression) =
                if expr.Expression :? ConstantExpression then
                    
                    let obj = (expr.Expression :?> ConstantExpression).Value
                    
                    let (value, p) = 
                        match expr.Member.MemberType with
                        | MemberTypes.Field ->
                            let fi = expr.Member :?> FieldInfo
                            fi.GetValue(obj), Expression.Parameter(expr.Type, sprintf "___param%s___" fi.Name) 
                        | MemberTypes.Property ->
                            let pi = expr.Member :?> PropertyInfo
                            let indexed = pi.GetIndexParameters() |> Seq.cast<obj> |> Seq.toArray
                            pi.GetValue(obj, indexed), Expression.Parameter(expr.Type, sprintf "___param%s___" pi.Name) 
                        | _ -> 
                            failwithf "Internal error : Accessing non Field or Property from MemberExpression %A" expr
                                                
                    if not <| this.Environment.ContainsKey(p.Name) then
                        this.Environment.Add(p.Name, (p, value))
                    p :> _
                else
                    expr.Update(this.Visit expr.Expression) :> _

    module ArgsCollector =
        let apply (vars : seq<ParameterExpression>) (expr : Expression) =
            let localVars = FreeVariablesVisitor.getLocals expr
            let clv = new ArgsCollectorVisitor(vars, localVars)
            let expr = clv.Visit(expr)
            let values = clv.Environment.Values
            expr, (values |> Seq.map fst |> Seq.toArray), (values |> Seq.map snd |> Seq.toArray)

