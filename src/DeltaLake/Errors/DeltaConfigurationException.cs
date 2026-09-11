using System;

namespace DeltaLake.Errors
{
    /// <summary>
    /// Represents a configuration error detected before interacting with the runtime
    /// </summary>
    /// <remarks>
    /// This is raised by client side argument validation, before any call into the runtime, so its
    /// inherited <see cref="DeltaLakeException.ErrorCode"/> is a sentinel rather than a runtime error
    /// code. Catch this type to detect a caller error; do not classify it by code.
    /// </remarks>
    public class DeltaConfigurationException : DeltaLakeException
    {

        /// <summary>
        ///  Initializes a new instance of the DeltaConfigurationException class.
        /// </summary>
        /// <param name="message">The message that describes the error</param>
        public DeltaConfigurationException(string? message)
            : base(message, 1000)
        {
        }

        /// <summary>
        ///  Initializes a new instance of the DeltaConfigurationException class
        ///  and a reference to the inner exception that is the cause of this exception
        /// </summary>
        /// <param name="message">The message that describes the error</param>
        /// <param name="innerException">The exception that is the cause of the current exception, or a null reference</param>
        public DeltaConfigurationException(string? message, Exception? innerException)
            : base(message, 1000, innerException)
        {
        }
    }
}