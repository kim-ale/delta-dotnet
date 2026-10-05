using System.Runtime.InteropServices;

namespace DeltaLake.Kernel.Interop
{
    [StructLayout(LayoutKind.Sequential)]
    internal unsafe struct CDvInfo
    {
        public DvInfo* info;
        private byte hasVector;

        public bool has_vector => hasVector != 0;
    }
}