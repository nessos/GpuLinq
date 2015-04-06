using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Nessos.GpuLinq.Core;
using Nessos.GpuLinq.CSharp;

namespace DotProduct.CSharp
{
    class Program
    {
        static void Main(string[] args)
        {
            int size = 1000000;
            Random random = new Random();
            var input = Enumerable.Range(1, size).Select(x => (float)random.NextDouble()).ToArray();
            Stopwatch timer = new Stopwatch();
            float[] gpuResult;
            using (var context = new GpuContext())
            {   
                using (var xs = context.CreateGpuArray(input))
                {
                    // Dot Product
                    var kernel = context.Compile<IGpuArray<float>, float>(_xs => GpuQueryExpr.Zip(_xs, _xs, (a, b) => a * b).Sum());
                    timer.Start();
                    gpuResult = Enumerable.Range(1, 100).Select(i => context.Run(kernel, xs)).ToArray(); 
                    timer.Stop();
                    var elapsed = timer.ElapsedTicks;
                    Console.WriteLine("GPU Computation w/o compilation took {0} ticks", elapsed/100.0);
                    Console.WriteLine("Result: {0}", gpuResult[0]);
                }
                
                //var elapsed = timer.ElapsedMilliseconds;
                //Console.WriteLine("GPU Computation took {0} milliseconds", elapsed);
                
            }

            timer.Restart();
            var res = Enumerable.Range(1, 100).Select(i =>input.Zip(input, (a, b) => a * b).Sum()).ToArray();
            timer.Stop();
            var elapsed2 = timer.ElapsedTicks;
            Console.WriteLine("CPU Computation took {0} ticks", elapsed2/100.0);
            if (res[0] == gpuResult[0])
                Console.WriteLine("Cpu and Gpu computations are identical");
            else
                Console.WriteLine("Cpu and Gpu computations are different");
            Console.ReadLine();
        }
        
    }
}
