using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Nessos.GpuLinq.Base;
using Nessos.GpuLinq.CSharp;
using Nessos.GpuLinq.Core;
using FMath = Nessos.GpuLinq.Core.Functions.Math;
using System.Runtime.InteropServices;
namespace BlackScholes.CSharp
{
    class Program
    {

        [StructLayout(LayoutKind.Sequential)]
        struct InputData
        {
            public float Stock;
            public float Strike;
            public float Times;
        }
        [StructLayout(LayoutKind.Sequential)]
        struct OutPutData
        {
            public float Call;
            public float Put;
        }

        static void Main(string[] args)
        {
            // Based on https://github.com/gsvgit/Brahma.FSharp/tree/master/Source/BlackScholes
            const int optionCount = 1000000;
            const float riskFreeInterestRate = 0.02f;
            const float volatility = 0.30f;
            const float A1 = 0.31938153f;
            const float A2 =  -0.356563782f;
            const float A3 = 1.781477937f;
            const float A4 =  -1.821255978f;
            const float A5 = 1.330274429f;

            // Helper functions
            Expression<Func<float, float>> k = x => 1.0f / (1.0f + 0.2316419f * FMath.Abs(x));
            Expression<Func<float, float>> cnd = x =>
                    1.0f - 1.0f / FMath.Sqrt(2.0f * FMath.PI)
                    * FMath.Exp (-(FMath.Abs(x)) * (FMath.Abs(x)) / 2.0f) * (A1 * k.Invoke(x) + A2 * k.Invoke(x) * k.Invoke(x) + A3 * FMath.Pow(k.Invoke(x), 3)
                    + A4 * FMath.Pow(k.Invoke(x), 4) + A5 * FMath.Pow(k.Invoke(x), 4));

            Expression<Func<float, float>> cumulativeNormalDistribution = x => (x < 0.0f) ? 1.0f - cnd.Invoke(x) : cnd.Invoke(x);

            Expression<Func<float, float, float, float>> d1 =
                (stockPrice, strikePrice, timeToExpirationYears) =>
                    FMath.Log(stockPrice / strikePrice) + (riskFreeInterestRate + volatility * volatility / 2.0f) * timeToExpirationYears
                    / (volatility * FMath.Sqrt(timeToExpirationYears));

            Expression<Func<float, float, float>> d2 =
                (_d1, timeToExpirationYears) => _d1 - volatility * FMath.Sqrt(timeToExpirationYears);

            Expression<Func<float, float, float, float, float, float>> blackScholesCallOption =
                (_d1, _d2, stockPrice, strikePrice, timeToExpirationYears) =>
                    stockPrice * cumulativeNormalDistribution.Invoke(_d1) -
                    strikePrice * FMath.Exp(-(riskFreeInterestRate) * timeToExpirationYears) * cumulativeNormalDistribution.Invoke(_d2);

            Expression<Func<float, float, float, float, float, float>> blackScholesPutOption =
               (_d1, _d2, stockPrice, strikePrice, timeToExpirationYears) =>
                  strikePrice * FMath.Exp(-riskFreeInterestRate * timeToExpirationYears) * 
                  cumulativeNormalDistribution.Invoke(-_d2) - stockPrice * cumulativeNormalDistribution.Invoke(-_d1);
        
            // Init data
            var random = new System.Random();
            var data = Enumerable.Range(1, optionCount).Select(_ => new InputData
                                                                        {
                                                                            Stock = random.Random(5.0f, 30.0f),
                                                                            Strike = random.Random(1.0f, 100.0f),
                                                                            Times = random.Random(0.25f, 10.0f)
                                                                        }).ToArray();
            // Main code
            using (GpuContext context = new GpuContext())
            {
                using(var _data = context.CreateGpuArray(data))
                {
                    var query =
                        (from d in _data.AsGpuQueryExpr()
                         let _d1 = d1.Invoke(d.Stock, d.Strike, d.Times)
                         let _d2 = d2.Invoke(_d1, d.Times)
                         select new OutPutData { Call = blackScholesCallOption.Invoke(_d1, _d2, d.Stock, d.Strike, d.Times),
                                                 Put = blackScholesPutOption.Invoke(_d1, _d2, d.Stock, d.Strike, d.Times)
                         }).ToArray();

                    var result = context.Run(query);
                }
            }
        }
    }
    #region Helpers
    public static class RandomExtensions
    {
        public static float Random(this Random random, float low, float high)
        {
            var lerp = (float)random.NextDouble();
            return (1f - lerp) * low + lerp * high;
        }
    }
    #endregion

}
