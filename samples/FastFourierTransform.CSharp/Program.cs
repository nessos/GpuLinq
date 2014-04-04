using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Nessos.GpuLinq.Base;
using Nessos.GpuLinq.Core;
using Nessos.GpuLinq.CSharp;

namespace FastFourierTransform.CSharp
{
    class Program
    {
        

        [StructLayout(LayoutKind.Sequential)]
        struct Complex
        {
            public double A;
            public double B;
        }
        static void Main(string[] args)
        {
            int size = 8388608;
            // Input Data
            Random random = new Random();
            var input = Enumerable.Range(1, size).Select(x => new Complex { A = random.NextDouble(), B = 0.0 }).ToArray();
            var output = Enumerable.Range(1, size).Select(x => new Complex { A = 0.0, B = 0.0 }).ToArray();
            var xs = Enumerable.Range(0, size - 1).ToArray();
            
            using(var context = new GpuContext())
            {
                using (var _xs = context.CreateGpuArray(xs))
                {
                    var _input = context.CreateGpuArray(input);
                    var _output = context.CreateGpuArray(output);
                    // Forward FFT
                    int fftSize = 2;
                    for (int i = 0; i < System.Math.Log(size, 2.0); i++)
                    {
                        var query = (from x in _xs.AsGpuQueryExpr()
                                     let b = (((int)System.Math.Floor((double)x / fftSize)) * (fftSize / 2))
                                     let offset = x % (fftSize / 2)
                                     let x0 = b + offset
                                     let x1 = x0 + size / 2
                                     let val0 = _input[x0]
                                     let val1 = _input[x1]
                                     let angle = -2 * System.Math.PI * (x / fftSize)
                                     let t = new Complex { A = System.Math.Cos(angle), B = System.Math.Sin(angle) }
                                     select new Complex
                                     {
                                         A = val0.A + t.A * val1.A - t.B * val1.B,
                                         B = val0.B + t.B * val1.A + t.A * val1.B
                                     });
                        fftSize *= 2;
                        context.Fill(query, _output);
                        Swap(ref _input, ref _output);
                    }
                }
            }
        }
        #region Helpers
        private static void Swap<T>(ref T a, ref T b)
        {
            var temp = a;
            a = b;
            b = temp;
        }
        #endregion
    }
}
