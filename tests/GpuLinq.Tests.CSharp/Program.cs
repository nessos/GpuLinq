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
            public float Real;
            public float Img;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct Triple
        {
            public int X;
            public int Y;
            public int Step;
        }

        public static void Main(string[] args)
        {
            {

                Expression<Func<Complex, float>> squareLength = complex => complex.Real * complex.Real + complex.Img * complex.Img;
                Expression<Func<Complex, Complex, Complex>> mult = 
                    (c1, c2) => new Complex { Real =  c1.Real * c2.Real - c1.Img * c2.Img, 
                                              Img =  c1.Real * c2.Img + c2.Real * c1.Img };
                Expression<Func<Complex, Complex, Complex>> add =
                    (c1, c2) => new Complex { Real = c1.Real + c2.Real, Img = c1.Img + c2.Img };

                int[] ys = Enumerable.Range(0, 312).ToArray();
                int[] xs = Enumerable.Range(0, 534).ToArray();
                Triple[] output = Enumerable.Range(1, xs.Length * ys.Length).Select(_ => new Triple()).ToArray();

                using (var context = new GpuContext())
                {
                    using (var _xs = context.CreateGpuArray(xs))
                    {
                        using (var _ys = context.CreateGpuArray(ys))
                        {
                            using (var _output = context.CreateGpuArray(output))
                            {
                                float _ymin = 0;
                                float _xmin = 1;
                                float _step = 2;
                                int max_iters = 10;
                                float limit = 12;
                                var query =
                                     (from yp in _ys.AsGpuQueryExpr()
                                     from xp in _xs
                                     let _y = _ymin + _step * yp
                                     let _x = _xmin + _step * xp
                                     let c = new Complex { Real = _x, Img = _y }
                                     let iters = EnumerableEx.Generate(c, x => squareLength.Invoke(x) < limit,
                                                              x => add.Invoke(mult.Invoke(x, x), c), x => x)
                                                             .Take(max_iters)
                                                             .Count()
                                     select new Triple { X = xp, Y = yp, Step = iters });


                                Measure(() =>
                                    {
                                        for (int i = 0; i < 250; i++)
                                        {
                                            context.Run(query);
                                            
                                        }
                                    });
                            }
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
