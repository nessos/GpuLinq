namespace Nessos.GpuLinq.Core.Functions

open System

///Provides single precision constants and static methods for trigonometric, logarithmic, and other common mathematical functions.
///Note that all of the are stamp methods and should only be used from GpuLinq kernels.
type Math =
    ///Represents the ratio of the circumference of a circle to its diameter, specified by the constant, π.
    static member PI : Single = 3.14159265358979323846f
    
    /// This is a stamp method. See System.Math for more information.
    static member Cos(d : Single) : Single =
        invalidOp "This method should be used only from GpuLinq kernels."

    /// This is a stamp method. See System.Math for more information.
    static member Sin(d : Single) : Single =
        invalidOp "This method should be used only from GpuLinq kernels."
    
    /// This is a stamp method. See System.Math for more information.
    static member Floor(d : Single) : Single =
        invalidOp "This method should be used only from GpuLinq kernels."

    /// This is a stamp method. See System.Math for more information.
    static member Sqrt(d : Single) : Single =
        invalidOp "This method should be used only from GpuLinq kernels."

    /// This is a stamp method. See System.Math for more information.
    static member Exp(d : Single) : Single =
        invalidOp "This method should be used only from GpuLinq kernels."

    /// This is a stamp method. See System.Math for more information.
    static member Pow(x : Single, y : Single) : Single =
        invalidOp "This method should be used only from GpuLinq kernels."

    /// This is a stamp method. See System.Math for more information.
    static member Abs(d : Single) : Single =
        invalidOp "This method should be used only from GpuLinq kernels."

    /// This is a stamp method. See System.Math for more information.
    static member Log(d : Single) : Single =
        invalidOp "This method should be used only from GpuLinq kernels."
