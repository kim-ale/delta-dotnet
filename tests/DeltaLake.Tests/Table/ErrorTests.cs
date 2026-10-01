using System.Reflection;
using System.Text.RegularExpressions;
using DeltaLake.Errors;
using DeltaLake.Kernel.Callbacks.Errors;
using DeltaLake.Kernel.Interop;

namespace DeltaLake.Tests.Table;

public class ErrorTests
{
    [Theory]
    [InlineData(DeltaTableErrorCode.Utf8)]
    [InlineData(DeltaTableErrorCode.PartitionError)]
    [InlineData(DeltaTableErrorCode.Generic)]
    [InlineData(DeltaTableErrorCode.Kernel)]
    [InlineData(DeltaTableErrorCode.InvalidTimestamp)]
    public void DeltaRuntimeException_Code_Matches_ErrorCode(DeltaTableErrorCode code)
    {
        var exception = new DeltaRuntimeException("boom", (int)code);

        Assert.Equal(code, exception.Code);
        Assert.Equal((int)code, exception.ErrorCode);
    }

    /// <summary>
    /// The enum is excluded from binding generation and declared by hand so it can live in
    /// <c>DeltaLake.Errors</c>, so nothing regenerates it when the bridge changes. This pins it to the
    /// C header that the bridge actually publishes.
    /// </summary>
    [Fact]
    public void DeltaTableErrorCode_Matches_The_Bridge_Header()
    {
        var published = Enum.GetValues<DeltaTableErrorCode>()
            .ToDictionary(value => value.ToString(), value => (long)value);

        Assert.Equal(ReadErrorCodesFromHeader(), published);
    }

    private static Dictionary<string, long> ReadErrorCodesFromHeader()
    {
        using var stream = Assembly.GetExecutingAssembly()
            .GetManifestResourceStream("delta-lake-bridge.h")
            ?? throw new InvalidOperationException("The bridge header was not embedded in the test assembly.");
        using var reader = new StreamReader(stream);
        var header = reader.ReadToEnd();

        var block = Regex.Match(header, @"typedef enum DeltaTableErrorCode \{(.*?)\}", RegexOptions.Singleline);
        Assert.True(block.Success, "Could not locate the DeltaTableErrorCode enum in the bridge header.");

        return Regex.Matches(block.Groups[1].Value, @"(\w+)\s*=\s*(\d+)")
            .ToDictionary(match => match.Groups[1].Value, match => long.Parse(match.Groups[2].Value));
    }

    [Fact]
    public void DeltaConfigurationException_Does_Not_Expose_A_Typed_Code()
    {
        var exception = new DeltaConfigurationException("invalid");

        Assert.IsNotType<DeltaRuntimeException>(exception);
        Assert.False(Enum.IsDefined(typeof(DeltaTableErrorCode), exception.ErrorCode));
    }

    [Theory]
    [InlineData(KernelError.ObjectStoreError)]
    [InlineData(KernelError.IOErrorError)]
    [InlineData(KernelError.ReqwestError)]
    [InlineData(KernelError.FileAlreadyExists)]
    public void KernelException_Can_Be_Subclassed_To_Test_Classification(KernelError errorCode)
    {
        KernelException exception = new FakeKernelException("boom", errorCode);

        Assert.Equal(errorCode, exception.ErrorCode);
        Assert.Equal("boom", exception.Message);
        Assert.Equal("boom", exception.KernelMessage);
    }

    [Fact]
    public void A_Subclassed_KernelException_Is_Caught_As_A_KernelException()
    {
        static void Throw() => throw new FakeKernelException("boom", KernelError.ReqwestError);

        var caught = Assert.Throws<FakeKernelException>(Throw);

        Assert.IsAssignableFrom<KernelException>(caught);
    }

    /// <summary>
    /// Mirrors the test double a consumer has to write, since only the kernel raises the real thing.
    /// </summary>
    private sealed class FakeKernelException : KernelException
    {
        public FakeKernelException(string? message, KernelError errorCode)
            : base(message, errorCode)
        {
        }
    }
}
