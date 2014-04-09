using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Nessos.LinqOptimizer.Base;
using Nessos.LinqOptimizer.CSharp;

namespace Algorithms
{
    public struct MyComplex
    {
        readonly double real;
        readonly double imaginary;

        public MyComplex(double real, double imaginary)
        {
            this.real = real;
            this.imaginary = imaginary;
        }

        public static MyComplex operator +(MyComplex c1, MyComplex c2)
        {
            return new MyComplex(c1.real + c2.real, c1.imaginary + c2.imaginary);
        }

        public static MyComplex operator *(MyComplex c1, MyComplex c2)
        {
            return new MyComplex(c1.real * c2.real - c1.imaginary * c2.imaginary,
                                c1.real * c2.imaginary + c2.real * c1.imaginary);
        }

        public double SquareLength
        {
            get { return real * real + imaginary * imaginary; }
        }
    }

    public struct Triple
    {
        public int X;
        public int Y;
        public int Step;
    }

    internal class ScalarLinqRenderer : FractalRenderer
    {

        public ScalarLinqRenderer(Action<int, int, int> dp, Func<bool> abortFunc)
            : base(dp, abortFunc)
        {
            int[] ys = Enumerable.Range(0, 312).ToArray();
            int[] xs = Enumerable.Range(0, 534).ToArray();

            func = Extensions.Compile<float, float, float, Triple[]>(
                    (_ymin, _xmin, _step) =>
                        (from yp in ys.AsParallelQueryExpr()
                        from xp in xs
                        let _y = _ymin + _step * yp
                        let _x = _xmin + _step * xp
                        let c = new MyComplex(_x, _y)
                        let iters = EnumerableEx.Generate(c, x => x.SquareLength < limit, x => x * x + c, x => x)
                                                .Take(max_iters)
                                                .Count()
                        select new Triple { X = xp, Y = yp, Step = iters }).ToArray()
                    );
        }

        protected const float limit = 4.0f;

        readonly Func<float, float, float, Triple[]> func;

        public void RenderSingleThreadedWithLinq(float xmin, float xmax, float ymin, float ymax, float step)
        {

            var result = func(ymin, xmin, step);
            result.ForEach(m => DrawPixel(m.X, m.Y, m.Step));

            //var query =
            //    from yp in Enumerable.Range(0, (int)(((ymax - ymin) / step) + .5f)).AsQueryExpr()
            //    from xp in Enumerable.Range(0, (int)(((xmax - xmin) / step) + .5f))
            //    let _y = ymin + step * yp
            //    let _x = xmin + step * xp
            //    let c = new MyComplex(_x, _y)
            //    let iters = EnumerableEx.Generate(c, x => x.SquareLength < limit, x => x * x + c, x => x)
            //                            .Take(max_iters)
            //                            .Count()
            //    select Tuple.Create(xp, yp, iters);
            //query.ForEach(m => DrawPixel(m.Item1, m.Item2, m.Item3)).Run();

        }
    }
}