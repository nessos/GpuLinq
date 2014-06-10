using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nessos.GpuLinq;
using Nessos.GpuLinq.CSharp;
using FsCheck.Fluent;
using System.Threading;
using System.Runtime.InteropServices;
using Nessos.GpuLinq.Core;
using System.Linq.Expressions;
using SMath = Nessos.GpuLinq.Core.Functions.Math;

using OpenCL.Net.Extensions;
using OpenCL.Net;
using System.IO;
using Nessos.GpuLinq.Base;


namespace Nessos.GpuLinq.Tests.CSharp
{
    [TestFixture]
    class GpuQueryTests
    {
        static string platformWildCard = "*";

        [Test]
        public void Select()
        {
            using (var context = new GpuContext(platformWildCard))
            {
                Spec.ForAny<int[]>(xs =>
                {
                    using (var _xs = context.CreateGpuArray(xs))
                    {
                        var x = context.Run(_xs.AsGpuQueryExpr().Select(n => n * 2).ToArray());
                        var y = xs.Select(n => n * 2).ToArray();
                        return x.SequenceEqual(y);
                    }
                }).QuickCheckThrowOnFailure();
            }
        }


        [Test]
        public void Pipelined()
        {
            using (var context = new GpuContext(platformWildCard))
            {
                Spec.ForAny<int[]>(xs =>
                {
                    using (var _xs = context.CreateGpuArray(xs))
                    {

                        var x = context.Run(_xs.AsGpuQueryExpr()
                                              .Select(n => (float)n * 2)
                                              .Select(n => n + 1).ToArray());
                        var y = xs
                                .Select(n => (float)n * 2)
                                .Select(n => n + 1).ToArray();

                        return x.SequenceEqual(y);
                    }

                }).QuickCheckThrowOnFailure();
            }
        }

        [Test]
        public void Where()
        {
            using (var context = new GpuContext(platformWildCard))
            {
                Spec.ForAny<int[]>(xs =>
                {

                    using (var _xs = context.CreateGpuArray(xs))
                    {

                        var x = context.Run((from n in _xs.AsGpuQueryExpr()
                                            where n % 2 == 0
                                            select n + 1).ToArray());
                        var y = (from n in xs
                                 where n % 2 == 0
                                 select n + 1).ToArray();

                        return x.SequenceEqual(y);
                    }
                }).QuickCheckThrowOnFailure();
            }
        }

        [Test]
        public void Sum()
        {
            using (var context = new GpuContext(platformWildCard))
            {

                Spec.ForAny<int[]>(xs =>
                {
                    using (var _xs = context.CreateGpuArray(xs))
                    {

                        var x = context.Run((from n in _xs.AsGpuQueryExpr()
                                             select n + 1).Sum());
                        var y = (from n in xs
                                 select n + 1).Sum();

                        return x == y;
                    }
                }).QuickCheckThrowOnFailure();
                
            }
        }

        [Test]
        public void Count()
        {
            using (var context = new GpuContext(platformWildCard))
            {

                Spec.ForAny<int[]>(xs =>
                {
                    using (var _xs = context.CreateGpuArray(xs))
                    {
                        var x = context.Run((from n in _xs.AsGpuQueryExpr()
                                             where n % 2 == 0
                                             select n + 1).Count());

                        var y = (from n in xs
                                 where n % 2 == 0
                                 select n + 1).Count();

                        return x == y;
                    }
                }).QuickCheckThrowOnFailure();
            }
        }

        [Test]
        public void ToArray()
        {
            using (var context = new GpuContext(platformWildCard))
            {
                Spec.ForAny<int[]>(xs =>
                {
                    using (var _xs = context.CreateGpuArray(xs))
                    {
                        var x = context.Run(_xs.AsGpuQueryExpr().Select(n => n * 2).ToArray());
                        var y = xs.Select(n => n * 2).ToArray();
                        return x.SequenceEqual(y);
                    }
                }).QuickCheckThrowOnFailure();
            }
        }

        [Test]
        public void Zip()
        {
            using (var context = new GpuContext(platformWildCard))
            {
                Spec.ForAny<int[]>(ms =>
                {
                    using (var _ms = context.CreateGpuArray(ms))
                    {
                        var xs = context.Run(GpuQueryExpr.Zip(_ms, _ms, (a, b) => a * b).Select(x => x + 1).ToArray());
                        var ys = Enumerable.Zip(ms, ms, (a, b) => a * b).Select(x => x + 1).ToArray();

                        return xs.SequenceEqual(ys);
                    }
                }).QuickCheckThrowOnFailure();
            }
        }

        [Test]
        public void ZipWithFilter()
        {
            using (var context = new GpuContext(platformWildCard))
            {
                Spec.ForAny<int[]>(ms =>
                {
                    using (var _ms = context.CreateGpuArray(ms))
                    {
                        var xs = context.Run(GpuQueryExpr.Zip(_ms, _ms, (a, b) => a * b).Where(x => x % 2 == 0).ToArray());
                        var ys = Enumerable.Zip(ms, ms, (a, b) => a * b).Where(x => x % 2 == 0).ToArray();

                        return xs.SequenceEqual(ys);
                    }
                }).QuickCheckThrowOnFailure();
            }
        }

        [Test]
        public void ZipWithReduction()
        {
            using (var context = new GpuContext(platformWildCard))
            {
                Spec.ForAny<int[]>(ms =>
                {
                    using (var _ms = context.CreateGpuArray(ms))
                    {
                        // Dot Product
                        var xs = context.Run(GpuQueryExpr.Zip(_ms, _ms, (a, b) => a * b).Sum());
                        var ys = Enumerable.Zip(ms, ms, (a, b) => a * b).Sum();

                        return xs == ys;
                    }
                }).QuickCheckThrowOnFailure();
            }
        }

        [Test]
        public void LinqLet()
        {
            using (var context = new GpuContext(platformWildCard))
            {
                Spec.ForAny<int[]>(nums =>
                {
                    using (var _nums = context.CreateGpuArray(nums))
                    {
                        var x =
                            context.Run((from num in _nums.AsGpuQueryExpr()
                                         let a = num * 2
                                         let c = a + 1
                                         let b = a * 2
                                         let e = b - 5
                                         let d = c * c
                                         let m = 3
                                         select a + b + c + d + e + m + num).Sum());

                        var y =
                            (from num in nums
                             let a = num * 2
                             let c = a + 1
                             let b = a * 2
                             let e = b - 5
                             let d = c * c
                             let m = 3
                             select a + b + c + d + e + m + num).Sum();

                        return x == y;
                    }
                }).QuickCheckThrowOnFailure();
                
            }
        }

        #region Struct Tests
        [StructLayout(LayoutKind.Sequential)]
        struct Node
        {
            public int x;
            public int y;
        }

        [Test]
        public void Structs()
        {
            using (var context = new GpuContext(platformWildCard))
            {
                Spec.ForAny<int[]>(nums =>
                {
                    var nodes = nums.Select(num => new Node { x = num, y = num }).ToArray();
                    using (var _nodes = context.CreateGpuArray(nodes))
                    {
                        var xs = context.Run((from node in _nodes.AsGpuQueryExpr()
                                             let x = node.x + 2
                                             let y = node.y + 1
                                             select new Node { x = x, y = y }).ToArray());
                        var ys = (from node in nodes
                                  let x = node.x + 2
                                  let y = node.y + 1
                                  select new Node { x = x, y = y }).ToArray();

                        return xs.SequenceEqual(ys);
                    }
                }).QuickCheckThrowOnFailure();
            }
        }
        #endregion

        [Test]
        public void ConstantLifting()
        {
            using (var context = new GpuContext(platformWildCard))
            {
                Spec.ForAny<int[]>(nums =>
                {
                    using (var _nums = context.CreateGpuArray(nums))
                    {
                        int c = nums.Length;
                        var xs = context.Run((from num in _nums.AsGpuQueryExpr()
                                             let y = num + c
                                             let k = y + c
                                             select c + y + k).ToArray());

                        var ys = (from num in nums
                                  let y = num + c
                                  let k = y + c
                                  select c + y + k).ToArray();
                        return xs.SequenceEqual(ys);
                    }
                }).QuickCheckThrowOnFailure();
            }
        }


        [Test]
        public void GpuArrayIndexer()
        {
            using (var context = new GpuContext(platformWildCard))
            {
                Spec.ForAny<int>(n =>
                {
                    if (n < 0) n = 0;
                    var nums = Enumerable.Range(0, n).ToArray();
                    using (var _nums = context.CreateGpuArray(nums))
                    {
                        using (var __nums = context.CreateGpuArray(nums))
                        {
                            int length = __nums.Length;
                            var xs = context.Run((from num in _nums.AsGpuQueryExpr()
                                                  let y = __nums[num % length]
                                                  select num + y).ToArray());

                            var ys = (from num in nums
                                      let y = nums[num % length]
                                      select num + y).ToArray();

                            return xs.SequenceEqual(ys);
                        }
                    }
                }).QuickCheckThrowOnFailure();
            }
        }

        [Test]
        public void MathFunctionsSingle()
        {
            using (var context = new GpuContext(platformWildCard))
            {
                Spec.ForAny<int[]>(xs =>
                {
                    using (var _xs = context.CreateGpuArray(xs))
                    {

                        var gpuResult = context.Run((from n in _xs.AsGpuQueryExpr()
                                                     let pi = SMath.PI
                                                     let c = SMath.Cos(n)
                                                     let s = SMath.Sin(n)
                                                     let f = SMath.Floor(pi)
                                                     let sq = SMath.Sqrt(n * n)
                                                     let ex = SMath.Exp(pi)
                                                     let p = SMath.Pow(pi, 2)
                                                     let a = SMath.Abs(c)
                                                     let l = SMath.Log(n)
                                                     select f * pi * c * s * sq * ex * p * a * l).ToArray());

                        var openClResult = this.MathFunctionsSingleTest(xs);


                        return gpuResult.Zip(openClResult, (x, y) => (float.IsNaN(x) && float.IsNaN(y)) ? true : System.Math.Abs(x - y) < 0.001)
                                        .SequenceEqual(Enumerable.Range(1, xs.Length).Select(_ => true));
                    }
                }).QuickCheckThrowOnFailure();
            }
        }

        [Test]
        public void Fill()
        {
            using (var context = new GpuContext(platformWildCard))
            {
                Spec.ForAny<int[]>(xs =>
                {
                    using (var _xs = context.CreateGpuArray(xs))
                    {
                        using (var _out = context.CreateGpuArray(Enumerable.Range(1, xs.Length).ToArray()))
                        {
                            context.Fill(_xs.AsGpuQueryExpr().Select(n => n * 2), _out);
                            var y = xs.Select(n => n * 2).ToArray();
                            return _out.ToArray().SequenceEqual(y);
                        }
                    }
                }).QuickCheckThrowOnFailure();
            }
        }


        [Test]
        public void FunctionSplicing()
        {
            using (var context = new GpuContext(platformWildCard))
            {
                Spec.ForAny<int[]>(xs =>
                {
                    using (var _xs = context.CreateGpuArray(xs))
                    {
                        Expression<Func<int, int>> g = x => x + 1;
                        Expression<Func<int, int>> f = x => 2 * g.Invoke(x);
                        Expression<Func<int, int>> h = x => g.Invoke(x) + f.Invoke(1);
                        var query = (from x in _xs.AsGpuQueryExpr()
                                     let m = h.Invoke(x)
                                     let k = f.Invoke(x)
                                     let y = g.Invoke(x)
                                     select k).ToArray();

                        var gpuResult = context.Run(query);

                        Func<int, int> _g = x => x + 1;
                        Func<int, int> _f = x => 2 * _g.Invoke(x);
                        var cpuResult = (from x in xs
                                         select _f.Invoke(x)).ToArray();

                        return gpuResult.SequenceEqual(cpuResult);
                    }
                }).QuickCheckThrowOnFailure();
            }
        }

        [Test]
        public void FunctionSplicingVariadic()
        {
            using (var context = new GpuContext(platformWildCard))
            {
                Spec.ForAny<int[]>(xs =>
                {
                    using (var _xs = context.CreateGpuArray(xs))
                    {
                        Expression<Func<int, int>> g = x => x + 1;
                        Expression<Func<int, int, int>> f = (x,y) => y * g.Invoke(x);
                        Expression<Func<int, int, int, int>> h = (x,y,z) => g.Invoke(x) + f.Invoke(y,z);
                        Expression<Func<int, int, int, int, int>> i = (x,y,z,w) => g.Invoke(x) + f.Invoke(x,y) + h.Invoke(x,y,z) + w;
                        var query = (from x in _xs.AsGpuQueryExpr()
                                     let n = i.Invoke(x,x,x,x)
                                     let m = h.Invoke(x,x,x)
                                     let l = f.Invoke(x,x)
                                     let k = g.Invoke(x)
                                     select k).ToArray();

                        var gpuResult = context.Run(query);

                        Func<int, int> _g = x => x + 1;
                        var cpuResult = (from x in xs
                                         select _g.Invoke(x)).ToArray();

                        return gpuResult.SequenceEqual(cpuResult);
                    }
                }).QuickCheckThrowOnFailure();
            }
        }


        [Test]
        public void TernaryIfElse()
        {
            using (var context = new GpuContext(platformWildCard))
            {
                Spec.ForAny<int[]>(xs =>
                {
                    using (var _xs = context.CreateGpuArray(xs))
                    {

                        var query = (from n in _xs.AsGpuQueryExpr()
                                     where n > 10
                                     select (n % 2 == 0) ? 1 : 0).ToArray();

                        var gpuResult = context.Run(query);


                        var cpuResult = (from n in xs
                                         where n > 10
                                         select (n % 2 == 0) ? 1 : 0).ToArray();

                        return gpuResult.SequenceEqual(cpuResult);
                    }
                }).QuickCheckThrowOnFailure();
            }
        }

        [Test]
        public void SelectMany()
        {
            using (var context = new GpuContext(platformWildCard))
            {
                Spec.ForAny<int[]>(xs =>
                {
                    using (var _xs = context.CreateGpuArray(xs))
                    {
                        
                        var query = (from x in _xs.AsGpuQueryExpr()
                                     from _x in _xs
                                     select x * _x).ToArray();

                        var gpuResult = context.Run(query);


                        var cpuResult = (from x in xs
                                         from _x in xs
                                         select x * _x).ToArray();

                        return gpuResult.SequenceEqual(cpuResult);
                    }
                }).QuickCheckThrowOnFailure();
            }
        }


        [Test]
        public void SelectManyWithLet()
        {
            using (var context = new GpuContext(platformWildCard))
            {
                Spec.ForAny<int[]>(xs =>
                {
                    using (var _xs = context.CreateGpuArray(xs))
                    {

                        var query = (from x in _xs.AsGpuQueryExpr()
                                     from _x in _xs
                                     let test = x * _x
                                     select test + 1).ToArray();

                        var gpuResult = context.Run(query);


                        var cpuResult = (from x in xs
                                         from _x in xs
                                         let test = x * _x
                                         select test + 1).ToArray();

                        return gpuResult.SequenceEqual(cpuResult);
                    }
                }).QuickCheckThrowOnFailure();
            }
        }

        [Test]
        public void InnerEnumerablePipeline()
        {
            using (var context = new GpuContext(platformWildCard))
            {
                Spec.ForAny<int[]>(xs =>
                {
                    using (var _xs = context.CreateGpuArray(xs))
                    {

                        var query = (from x in _xs.AsGpuQueryExpr()
                                     let y = x
                                     let test = EnumerableEx.Generate(1, i => i < y, i => i + 1, i => i)
                                                    .Take(10)
                                                    .Count()
                                     select test + 1).ToArray();

                        var gpuResult = context.Run(query);


                        var cpuResult = (from x in xs
                                         let y = x
                                         let test = EnumerableEx.Generate(1, i => i < y, i => i + 1, i => i)
                                                    .Take(10)
                                                    .Count()
                                         select test + 1).ToArray();;

                        return gpuResult.SequenceEqual(cpuResult);
                    }
                }).QuickCheckThrowOnFailure();
            }
        }

        [Test]
        public void CompiledKernel()
        {
            using (var context = new GpuContext(platformWildCard))
            {
                Expression<Func<int, IGpuArray<int>, IGpuQueryExpr<int>>> queryExpr = (n, _xs) =>
                    _xs.AsGpuQueryExpr().Select(x => x * n).Sum();
                var kernel = context.Compile(queryExpr);
                int _n = 42;
                Spec.ForAny<int[]>(xs =>
                {
                    using (var _xs = context.CreateGpuArray(xs))
                    {
                        var gpuResult = context.Run(kernel, _n, _xs);
                        var cpuResult = xs.Select(x => x * _n).Sum();
                        return gpuResult == cpuResult;
                    }
                }).QuickCheckThrowOnFailure();
            }
        }

        #region Helpers
        OpenCL.Net.Environment env = platformWildCard.CreateCLEnvironment();

        public float [] MathFunctionsSingleTest(int[] input)
        {
            if(input.Length == 0) return new float[0];

            var source =
                @"#pragma OPENCL EXTENSION cl_khr_fp64 : enable

                __kernel void kernelCode(__global int* ___input___,  __global float* ___result___)
                {
                                
                    int n0;
                    float ___final___10;
                    int ___flag___11;
                    int ___id___ = get_global_id(0);
                    n0 = ___input___[___id___];
                                
                    float pi = 3.14159274f;
                    float c = cos(((float) n0));
                    float s = sin(((float) n0));
                    float f = floor(pi);
                    float sq = sqrt(((float) (n0 * n0)));
                    float ex = exp(pi);
                    float p = powr(pi, 2.0f);
                    float a = fabs(c);
                    float l = log(((float) n0));
                    ___final___10 = ((((((((f * pi) * c) * s) * sq) * ex) * p) * a) * l);
                    ___result___[___id___] = ___final___10;
                }
                ";
            var output = new float[input.Length];
            ErrorCode error;
            var a = Cl.CreateBuffer(env.Context, MemFlags.ReadOnly | MemFlags.None | MemFlags.UseHostPtr, (IntPtr)(input.Length * sizeof(int) ), input, out error);
            var b = Cl.CreateBuffer(env.Context, MemFlags.WriteOnly | MemFlags.None | MemFlags.UseHostPtr, (IntPtr)(input.Length * sizeof(float)), output, out error);
            var max = Cl.GetDeviceInfo(env.Devices[0], DeviceInfo.MaxWorkGroupSize, out error).CastTo<uint>();
            OpenCL.Net.Program program = Cl.CreateProgramWithSource(env.Context, 1u, new string[] { source }, null, out error);
            error = Cl.BuildProgram(program, (uint)env.Devices.Length, env.Devices, " -cl-fast-relaxed-math  -cl-mad-enable ", null, IntPtr.Zero);
            OpenCL.Net.Kernel kernel = Cl.CreateKernel(program, "kernelCode", out error);
            error = Cl.SetKernelArg(kernel, 0, a);
            error = Cl.SetKernelArg(kernel, 1, b);
            Event eventID;
            error = Cl.EnqueueNDRangeKernel(env.CommandQueues[0], kernel, (uint)1, null, new IntPtr[] { (IntPtr)input.Length }, new IntPtr[] { (IntPtr)1 }, (uint)0, null, out eventID);
            env.CommandQueues[0].ReadFromBuffer(b, output);
            a.Dispose();
            b.Dispose();
            //env.Dispose();
            return output;
        }
        #endregion

    }
}
