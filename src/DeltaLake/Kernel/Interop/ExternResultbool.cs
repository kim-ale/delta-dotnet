using System.Runtime.InteropServices;

namespace DeltaLake.Kernel.Interop
{
    [StructLayout(LayoutKind.Sequential)]
    internal unsafe struct ExternResultbool
    {
        public ExternResultbool_Tag tag;
        public AnonymousUnion Anonymous;

        [StructLayout(LayoutKind.Explicit)]
        internal struct AnonymousUnion
        {
            [FieldOffset(0)]
            public Success Anonymous1;

            [FieldOffset(0)]
            public Failure Anonymous2;
        }

        [StructLayout(LayoutKind.Sequential)]
        internal struct Success
        {
            private byte value;

            public bool ok => value != 0;
        }

        [StructLayout(LayoutKind.Sequential)]
        internal struct Failure
        {
            public EngineError* err;
        }
    }
}