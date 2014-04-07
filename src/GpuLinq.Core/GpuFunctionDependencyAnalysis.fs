
namespace Nessos.GpuLinq.Core
    
module GpuFunctionDependencyAnalysis =

    open System
    open System.Collections
    open System.Collections.Generic
    open System.Linq
    open System.Linq.Expressions
    open System.Reflection

    type private GpuFunctionDependencyVisitor (paramExprs : ParameterExpression []) =
        
        inherit ExpressionVisitor () with
            let isGpuInvoke (expr : MethodCallExpression) =
                expr.Method.Name = "Invoke" 
                && expr.Arguments.Count >= 1
                && expr.Arguments.[0].NodeType = ExpressionType.Parameter
                && paramExprs |> Array.exists (fun p -> p.Name = ((expr.Arguments.[0] :?> ParameterExpression).Name)) 
                && expr.Object = null

            let dependencies = new List<ParameterExpression>()
            
            override this.VisitMethodCall(expr : MethodCallExpression) : Expression =
                //Diagnostics.Debugger.Break()
                if isGpuInvoke expr then
                    dependencies.Add(expr.Arguments.[0] :?> ParameterExpression)
                expr :> _

            member this.GetDependencies () = dependencies
                                             |> Seq.groupBy(fun x -> x.Name)
                                             |> Seq.map (snd >> Seq.head)
                                             |> Seq.toArray
    
    let private find (body : Expression, paramExprs : ParameterExpression []) =
        let v = new GpuFunctionDependencyVisitor(paramExprs)
        v.Visit(body) |> ignore
        v.GetDependencies()

    let sort (funcs : (ParameterExpression * ParameterExpression list * (Expression * ParameterExpression [] * obj [])) []) =
        let temp = funcs |> Array.map (fun (paramExpr, varExpr, (expr, paramExprs, objs)) -> paramExpr, expr)
        let paramExprs = temp |> Array.map fst
        let graph = new Dictionary<string, seq<ParameterExpression>>()
        temp
        |> Seq.map (fun (param, body) -> param, find(body, paramExprs))
        |> Seq.iter(fun (k,v) -> graph.[k.Name] <- v)


        // DFS/ topological sorting
        let visited = new HashSet<string>()

        let out =
            seq {
            for node in graph do
                if visited.Contains(node.Key) then ()
                else
                    let rec dfs (node : KeyValuePair<string, seq<ParameterExpression>>) =
                        seq {
                            for childNode in node.Value do
                                if not(visited.Contains(childNode.Name)) then
                                    let kvp = new KeyValuePair<_,_>(childNode.Name, graph.[childNode.Name])
                                    yield! dfs(kvp)
                            visited.Add(node.Key) |> ignore
                            yield node.Key 
                        }
                    yield! dfs node
            }

        let order =
            out |> Seq.mapi (fun i v -> v, i)
                |> Seq.toArray
                |> Map.ofSeq

        funcs |> Array.sortBy(fun (v,_,_) -> order.[v.Name])





