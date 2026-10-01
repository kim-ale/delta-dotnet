using System;

namespace DeltaLake.Errors
{
    /// <summary>
    /// Represents an error that occurs during interaction with the underlying runtime
    /// </summary>
    public class DeltaLakeException : Exception
    {
        /// <summary>
        ///  Initializes a new instance of the DeltaLakeException class.
        /// </summary>
        /// <param name="message">The message that describes the error</param>
        /// <param name="code">The code that describes the error</param>
        public DeltaLakeException(string? message, int code) : base(message)
        {
            ErrorCode = code;
        }


        /// <summary>
        ///  Initializes a new instance of the DeltaLakeException class
        ///  and a reference to the inner exception that is the cause of this exception
        /// </summary>
        /// <param name="message">The message that describes the error</param>
        /// <param name="code">The code that describes the error</param>
        /// <param name="innerException">The exception that is the cause of the current exception, or a null reference</param>
        public DeltaLakeException(string? message, int code, Exception? innerException)
        : base(message, innerException)
        {
            ErrorCode = code;
        }

        /// <summary>
        /// A unique error code coming from the delta lake runtime
        /// </summary>
        /// <remarks>
        /// Only <see cref="DeltaRuntimeException"/> carries a runtime error code. Other derived types
        /// use a sentinel outside that code space, so this value must not be cast to
        /// <see cref="DeltaTableErrorCode"/> unless the exception is a
        /// <see cref="DeltaRuntimeException"/>, which already exposes it typed as
        /// <see cref="DeltaRuntimeException.Code"/>.
        /// </remarks>
        public int ErrorCode { get; }
    }
}