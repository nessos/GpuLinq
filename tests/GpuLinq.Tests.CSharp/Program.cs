using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Nessos.GpuLinq;
using Nessos.GpuLinq.CSharp;
using System.Runtime.InteropServices;
using System.IO.MemoryMappedFiles;
using System.IO;
using Nessos.GpuLinq.Core;

namespace Nessos.GpuLinq.Tests.CSharp
{


    public class Program
    {
        [StructLayout(LayoutKind.Sequential)]
        public struct Complex
        {
            public double A;
            public double B;
        }

        public static void Main(string[] args)
        {

            //var size = Marshal.SizeOf(typeof(bool));

            //var xs = Enumerable.Range(1, 10000000).Select(x => x).ToArray();
            //using (var context = new GpuContext())
            //{

            //    using (var _xs = context.CreateGpuArray(xs))
            //    {
            //        Expression<Func<int, int>> g = x => 2 * x;
            //        Expression<Func<int, int>> f = x => 2 * g.Invoke(x);
            //        var query = (from n in _xs.AsGpuQueryExpr()
            //                     let x = f.Invoke(n)
            //                     let y = g.Invoke(n)
            //                     select (n % 2 == 0) ? 1 : 0).ToArray();

            //        var gpuResult = context.Run(query);

            //        var cpuResult =
            //            (from n in xs
            //             where n > 10
            //             select (n % 2 == 0) ? 1 : 0).ToArray();

            //        var result = gpuResult.SequenceEqual(cpuResult);
            //    }
            //}

            //var x = (new GpuQueryTests()).MathFunctionsSingleTest(new int[] { 0 });;
            (new GpuQueryTests()).FunctionSplicingVariadic();
        }

        static void Measure(Action action)
        {
            Measure(action, 1);
        }

        static void Measure(Action action, int iterations)
        {
            var watch = new Stopwatch();
            watch.Start();
            for (int i = 0; i < iterations; i++)
            {
                action();
            }
            Console.WriteLine(new TimeSpan(watch.Elapsed.Ticks / iterations));
        }
    }
}
