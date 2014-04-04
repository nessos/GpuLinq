using System;
using System.Collections.Generic;
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
            int size = 1000;
            Random random = new Random();
            var input = Enumerable.Range(1, size).Select(x => random.NextDouble()).ToArray();
            using (var context = new GpuContext())
            {

                using (var xs = context.CreateGpuArray(input))
                {
                    // Dot Product
                    var query = GpuQueryExpr.Zip(xs, xs, (a, b) => a * b).Sum();
                    var result = context.Run(query);
                    Console.WriteLine("Result: {0}", result);
                }
                
            }
        }
    }
}
