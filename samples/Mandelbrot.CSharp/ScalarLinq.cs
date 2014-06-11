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

    [StructLayout(LayoutKind.Sequential)]
    public struct Pair
    {
        public int X;
        public int Y;
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
        private readonly IGpuArray<Pair> _pairs;
        private readonly IGpuArray<int> _output;
        public ScalarLinqRenderer(Action<int, int, int> dp, Func<bool> abortFunc)
            : base(dp, abortFunc)
        {
            Pair[] pairs = Enumerable.Range(0, 312).SelectMany(y => Enumerable.Range(0, 534).Select(x => new Pair { X = x, Y = y })).ToArray();
            int[] output = Enumerable.Range(0, pairs.Length).ToArray();
            this.context = new GpuContext("*");
            this._pairs = context.CreateGpuArray(pairs);
            this._output = context.CreateGpuArray(output);

            parFunc = ParallelExtensions.Compile<float, float, float, int[]>(
                    (_ymin, _xmin, _step) =>
                        (from pair in pairs.AsParallelQueryExpr()
                        let _y = _ymin + _step * pair.Y
                        let _x = _xmin + _step * pair.X
                        let c = new MyComplex(_x, _y)
                        let iters = EnumerableEx.Generate(c, x => x.SquareLength < limit, x => x * x + c, x => x)
                                                .Take(max_iters)
                                                .Count()
                        select iters).ToArray()
                    );

            seqFunc = Extensions.Compile<float, float, float, int[]>(
                    (_ymin, _xmin, _step) =>
                        (from pair in pairs.AsQueryExpr()
                         let _y = _ymin + _step * pair.Y
                         let _x = _xmin + _step * pair.X
                         let c = new MyComplex(_x, _y)
                         let iters = EnumerableEx.Generate(c, x => x.SquareLength < limit, x => x * x + c, x => x)
                                                 .Take(max_iters)
                                                 .Count()
                         select iters).ToArray()
                    );
            gpuKernel = 
                context.Compile<float, float, float, IGpuArray<int>>(
                    (_ymin, _xmin, _step) =>
                         from pair in _pairs.AsGpuQueryExpr()
                         let _y = _ymin + _step * pair.Y
                         let _x = _xmin + _step * pair.X
                         let c = new Complex { Real = _x, Img = _y }
                         let iters = EnumerableEx.Generate(c, x => squareLength.Invoke(x) < limit,
                                                  x => add.Invoke(mult.Invoke(x, x), c), x => x)
                                                 .Take(max_iters)
                                                 .Count()
                         select iters);
        }

        protected const float limit = 4.0f;

        readonly Func<float, float, float, int[]> parFunc;
        readonly Func<float, float, float, int[]> seqFunc;
        readonly GpuKernel<float, float, float, IGpuArray<int>> gpuKernel;

        public void RenderMultiThreadedWithLinq(float xmin, float xmax, float ymin, float ymax, float step)
        {

            var result = parFunc(ymin, xmin, step);
            DrawPixels(result);
        }

        public void RenderWithGpuLinq(float xmin, float xmax, float ymin, float ymax, float step)
        {

            context.Fill(gpuKernel, _output, ymin, xmin, step);
            _output.Refresh();
            var array = _output.GetArray();
            DrawPixels(array);
            
        }

        private void DrawPixels(int[] array)
        {
            int i = 0;
            for (int y = 0; y < 312; y++)
            {
                for (int x = 0; x < 534; x++)
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