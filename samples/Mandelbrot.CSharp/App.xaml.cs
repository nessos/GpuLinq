// Project code based on http://code.msdn.microsoft.com/SIMD-Sample-f2c8c35a/sourcecode?fileId=112212&pathId=272559283

using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Numerics;
using System.Diagnostics;
using System.Threading;

namespace Mandelbrot
{
    /// <summary>
    /// Interaction logic for App.xaml
    /// </summary>
    public partial class App : Application
    {
        // Goofy magic to get SIMD working with the CTP
        // This will not be required once RyuJIT is official
        // You must use the type in the class constructor
        // It will not be accelerated in this function, though...
        static Vector<float> dummy;
        static App() { dummy = Vector<float>.One; }

        protected override void OnStartup(StartupEventArgs e)
        {
            base.OnStartup(e);

        }
    }
}
