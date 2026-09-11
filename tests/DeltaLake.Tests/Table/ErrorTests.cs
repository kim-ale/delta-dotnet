using DeltaLake.Bridge.Interop;
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

    [Fact]
    public void DeltaConfigurationException_Does_Not_Expose_A_Typed_Code()
    {
        var exception = new DeltaConfigurationException("invalid");

        Assert.IsNotType<DeltaRuntimeException>(exception);
        Assert.False(Enum.IsDefined(typeof(DeltaTableErrorCode), (uint)exception.ErrorCode));
    }

    [Theory]
    [InlineData(KernelError.ObjectStoreError)]
    [InlineData(KernelError.IOErrorError)]
    [InlineData(KernelError.ReqwestError)]
    [InlineData(KernelError.FileAlreadyExists)]
    public void KernelException_Can_Be_Constructed_With_An_Error_Code(KernelError errorCode)
    {
        var exception = new KernelException("boom", errorCode);

        Assert.Equal(errorCode, exception.ErrorCode);
        Assert.Equal("boom", exception.Message);
        Assert.Equal("boom", exception.KernelMessage);
    }
}
