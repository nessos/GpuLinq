namespace Nessos.GpuLinq.Core
    
open System
open System.Collections
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open System.Reflection
open Nessos.LinqOptimizer.Core



type private QuerySubExprVisitor (isValidQueryExpr : Expression -> bool) =
    inherit ExpressionVisitor () with
        
        let mutable querySubExprs = new List<Expression>()
        
        member this.GetSubExprs () = querySubExprs

        override this.Visit(expr : Expression) =
            if isValidQueryExpr(expr) then
                querySubExprs.Add(expr)
                expr
            else
                base.Visit(expr)

//type private LetBindingsVisitor () =
//
//    inherit ExpressionVisitor () with
//        
//        let mutable body = None
//        member this.GetBody () = body
//
//        override this.VisitBinary(expr : BinaryExpression) =
//            match expr with
//            | AnonymousTypeAssign(left, right) ->
//                //let first = right.Arguments.First() :?> ParameterExpression
//                body <- Some(right.Arguments.Last())
//                expr :> _
//            | _ -> 
//                expr.Update(this.Visit(expr.Left), null, this.Visit(expr.Right)) :> _

    
module QuerySubExpression =
    let get (isValidQueryExpr : Expression -> bool) (exprs : Expression list)  = 
        let qsv = new QuerySubExprVisitor(isValidQueryExpr)
        let exprs = 
            exprs 
            |> Seq.collect (fun expr -> qsv.Visit(expr) |> ignore; qsv.GetSubExprs())        
        exprs
        |> Seq.toList
        |> List.map(fun se ->
                let ce = CSharpExpressionOptimizer.Optimize(se)
                let (e, param) = FreeVariablesVisitor.getWithExpr(ce)
                let fExpr = Expression.Lambda(e, param)
                Expression.Parameter(fExpr.ReturnType, sprintf "func%d" (se.GetHashCode())), fExpr :> obj)
        |> List.unzip


//        match se with 
//        | None -> None
//        | Some se ->
//            //let lbv = new LetBindingsVisitor()
//            //lbv.Visit(expr) |> ignore
//            //let body = lbv.GetBody()
////            match body with
////            | None -> None
////            | Some body ->