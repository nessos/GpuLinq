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
using Nessos.GpuLinq.Base;

namespace Nessos.GpuLinq.Tests.CSharp
{


    public class Program
    {


        public static void Main(string[] args)
        {
            {

                var xs = Enumerable.Range(1, 1000000).Select(x => (long)x).ToArray();


                using (var context = new GpuContext("Intel*"))
                {
                    Expression<Func<int, IGpuArray<long>, IGpuQueryExpr<long>>> queryExpr = (n, _xs) =>
                                _xs.AsGpuQueryExpr().Select(x => x * n).Sum();


                    using (var _xs = context.CreateGpuArray(xs))
                    {
                        using (var kernel = context.Compile(queryExpr))
                        {
                            var result = context.Run(kernel, 2, _xs);
                            
                        }

                    }
                }
            }


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
