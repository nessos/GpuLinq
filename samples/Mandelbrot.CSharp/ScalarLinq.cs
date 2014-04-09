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

    internal class ScalarLinqRenderer : FractalRenderer
    {
        public ScalarLinqRenderer(Action<int, int, int> dp, Func<bool> abortFunc)
            : base(dp, abortFunc)
        {
        }

        protected const float limit = 4.0f;

        Func<Tuple<int[], int[], float, float, float>, IEnumerable<Tuple<int, int, int>>>
            func =
                Extensions.Compile<Tuple<int[], int[], float, float, float>, IEnumerable<Tuple<int, int, int>>>(
                    t =>
                        from yp in t.Item1.AsQueryExpr()
                        from xp in t.Item2
                        let _y = t.Item3 + t.Item5 * yp
                        let _x = t.Item4 + t.Item5 * xp
                        let c = new MyComplex(_x, _y)
                        let iters = EnumerableEx.Generate(c, x => x.SquareLength < limit, x => x * x + c, x => x)
                                                .Take(max_iters)
                                                .Count()
                        select Tuple.Create(xp, yp, iters)
                    );

        public void RenderSingleThreadedWithLinq(float xmin, float xmax, float ymin, float ymax, float step)
        {  
            var t = Tuple.Create(Enumerable.Range(0, (int)(((ymax - ymin) / step) + .5f)).ToArray(),
                                 Enumerable.Range(0, (int)(((xmax - xmin) / step) + .5f)).ToArray(),
                                 ymin,
                                 xmin,
                                 step);
            var result = func(t);

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

            result.ForEach(m => DrawPixel(m.Item1, m.Item2, m.Item3));
        }
    }
}