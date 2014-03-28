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
            

            var xs = Enumerable.Range(1, 10).Select(x => x).ToArray();
            using (var context = new GpuContext())
            {

                using (var _xs = context.CreateGpuArray(xs))
                {
                    Expression<Func<int, int>> g = x => x + 1;
                    Expression<Func<int, int>> f = x => 2 * g.Invoke(x);
                    var query = (from x in _xs.AsGpuQueryExpr()
                                 select f.Invoke(x)).ToArray();

                    context.Run(query);
                }
            }

            //(new GpuQueryTests()).ConstantLifting();
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
