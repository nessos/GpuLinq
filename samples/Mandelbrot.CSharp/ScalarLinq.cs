// Project code based on http://code.msdn.microsoft.com/SIMD-Sample-f2c8c35a/sourcecode?fileId=112212&pathId=272559283

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Nessos.GpuLinq.Core;
using Nessos.GpuLinq.CSharp;
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

    [StructLayout(LayoutKind.Sequential)]
    public struct Complex
    {
        public float Real;
        public float Img;
    }




    internal class ScalarLinqRenderer : FractalRenderer
    {
        Expression<Func<Complex, float>> squareLength = complex => complex.Real * complex.Real + complex.Img * complex.Img;
        Expression<Func<Complex, Complex, Complex>> mult =
            (c1, c2) => new Complex
            {
                Real = c1.Real * c2.Real - c1.Img * c2.Img,
                Img = c1.Real * c2.Img + c2.Real * c1.Img
            };
        Expression<Func<Complex, Complex, Complex>> add =
            (c1, c2) => new Complex { Real = c1.Real + c2.Real, Img = c1.Img + c2.Img };

        private readonly GpuContext context;
        private readonly IGpuArray<int> _xs;
        private readonly IGpuArray<int> _ys;
        private readonly IGpuArray<int> _output;
        public ScalarLinqRenderer(Action<int, int, int> dp, Func<bool> abortFunc)
            : base(dp, abortFunc)
        {
            int[] ys = Enumerable.Range(0, 312).ToArray();
            int[] xs = Enumerable.Range(0, 534).ToArray();
            int[] output = Enumerable.Range(0, 534 * 312).ToArray();
            this.context = new GpuContext();
            this._xs = context.CreateGpuArray(xs);
            this._ys = context.CreateGpuArray(ys);
            this._output = context.CreateGpuArray(output);

            parFunc = ParallelExtensions.Compile<float, float, float, int[]>(
                    (_ymin, _xmin, _step) =>
                        (from yp in ys.AsParallelQueryExpr()
                        from xp in xs
                        let _y = _ymin + _step * yp
                        let _x = _xmin + _step * xp
                        let c = new MyComplex(_x, _y)
                        let iters = EnumerableEx.Generate(c, x => x.SquareLength < limit, x => x * x + c, x => x)
                                                .Take(max_iters)
                                                .Count()
                        select iters).ToArray()
                    );

            seqFunc = Extensions.Compile<float, float, float, int[]>(
                    (_ymin, _xmin, _step) =>
                        (from yp in ys.AsQueryExpr()
                         from xp in xs
                         let _y = _ymin + _step * yp
                         let _x = _xmin + _step * xp
                         let c = new MyComplex(_x, _y)
                         let iters = EnumerableEx.Generate(c, x => x.SquareLength < limit, x => x * x + c, x => x)
                                                 .Take(max_iters)
                                                 .Count()
                         select iters).ToArray()
                    );
        }

        protected const float limit = 4.0f;

        readonly Func<float, float, float, int[]> parFunc;
        readonly Func<float, float, float, int[]> seqFunc;

        public void RenderMultiThreadedWithLinq(float xmin, float xmax, float ymin, float ymax, float step)
        {

            var result = parFunc(ymin, xmin, step);
            DrawPixels(result);
        }

        public void RenderWithGpuLinq(float xmin, float xmax, float ymin, float ymax, float step)
        {

            var query =
                 (from yp in _ys.AsGpuQueryExpr()
                  from xp in _xs
                  let _y = ymin + step * yp
                  let _x = xmin + step * xp
                  let c = new Complex { Real = _x, Img = _y }
                  let iters = EnumerableEx.Generate(c, x => squareLength.Invoke(x) < limit,
                                           x => add.Invoke(mult.Invoke(x, x), c), x => x)
                                          .Take(max_iters)
                                          .Count()
                  select iters);
            try
            {
                
                context.Fill(query, _output);
                _output.Refresh();
                var array = _output.GetArray();
                DrawPixels(array);
            }
            catch (Exception ex)
            {
                
            }
            
        }

        private void DrawPixels(int[] array)
        {
            int i = 0;
            for (int y = 0; y < _ys.Length; y++)
            {
                for (int x = 0; x < _xs.Length; x++)
                {
                    DrawPixel(x, y, array[i++]);
                }
            }
        }

        public void RenderSingleThreadedWithLinq(float xmin, float xmax, float ymin, float ymax, float step)
        {

            var result = seqFunc(ymin, xmin, step);
            DrawPixels(result);

        }
    }
}