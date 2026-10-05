using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using DeltaLake.Credentials;
using DeltaLake.Kernel.Callbacks.Errors;
using DeltaLake.Kernel.Credentials;
using DeltaLake.Kernel.Interop;

namespace DeltaLake.Tests.Table
{
    [Collection("KernelCredentialContract")]
    public class KernelCredentialRegistrationTests
    {
        private static readonly Func<IntPtr, uint> BeginRequest = CreatePointerCall("Begin", typeof(KernelCredentialRegistration));
        private static readonly Func<IntPtr, uint> RegisterDescriptor = CreatePointerCall("kernel_credential_register", typeof(KernelCredentialInterop));
        private static readonly Func<KernelCredentialRegistration, string, IReadOnlyCollection<KeyValuePair<string, string>>, IntPtr> NewEngine = CreateEngineCall();
        private static readonly Action<ulong, ulong, uint> CancelRequest = (Action<ulong, ulong, uint>)Delegate.CreateDelegate(
            typeof(Action<ulong, ulong, uint>), typeof(KernelCredentialRegistration).GetMethod("Cancel", BindingFlags.NonPublic | BindingFlags.Static)!);
        private static readonly AsyncLocal<string?> Ambient = new AsyncLocal<string?>();

        [DllImport("delta_kernel_ffi", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        private static extern void free_engine(IntPtr engine);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate uint ProbeBegin(IntPtr request);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void ProbeCancel(ulong contextId, ulong requestId, uint reason);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void ProbeReleased(ulong contextId);

        private static Func<IntPtr, uint> CreatePointerCall(string name, Type owner)
        {
            var target = owner.GetMethod(name, BindingFlags.NonPublic | BindingFlags.Static)!;
            var method = new DynamicMethod(name + "Test", typeof(uint), new[] { typeof(IntPtr) }, typeof(KernelCredentialRegistrationTests).Module, true);
            var code = method.GetILGenerator();
            code.Emit(OpCodes.Ldarg_0);
            code.Emit(OpCodes.Conv_U);
            code.Emit(OpCodes.Call, target);
            code.Emit(OpCodes.Ret);
            return (Func<IntPtr, uint>)method.CreateDelegate(typeof(Func<IntPtr, uint>));
        }

        private static Func<KernelCredentialRegistration, string, IReadOnlyCollection<KeyValuePair<string, string>>, IntPtr> CreateEngineCall()
        {
            var parameters = new[] { typeof(KernelCredentialRegistration), typeof(string), typeof(IReadOnlyCollection<KeyValuePair<string, string>>) };
            var method = new DynamicMethod("CreateCredentialEngineTest", typeof(IntPtr), parameters, typeof(KernelCredentialRegistrationTests).Module, true);
            var code = method.GetILGenerator();
            code.Emit(OpCodes.Ldarg_0);
            code.Emit(OpCodes.Ldarg_1);
            code.Emit(OpCodes.Ldarg_2);
            code.Emit(OpCodes.Call, typeof(KernelCredentialRegistration).GetMethod("CreateEngine", BindingFlags.NonPublic | BindingFlags.Instance)!);
            code.Emit(OpCodes.Conv_I);
            code.Emit(OpCodes.Ret);
            return (Func<KernelCredentialRegistration, string, IReadOnlyCollection<KeyValuePair<string, string>>, IntPtr>)method.CreateDelegate(
                typeof(Func<KernelCredentialRegistration, string, IReadOnlyCollection<KeyValuePair<string, string>>, IntPtr>));
        }

        private static uint Begin(KernelCredentialRequestV1 request)
        {
            var pointer = Marshal.AllocHGlobal(Marshal.SizeOf(typeof(KernelCredentialRequestV1)));
            try
            {
                Marshal.StructureToPtr(request, pointer, false);
                return BeginRequest(pointer);
            }
            finally
            {
                Marshal.FreeHGlobal(pointer);
            }
        }

        private static KernelCredentialRequestV1 Request(ulong contextId, ulong requestId = ulong.MaxValue)
            => new KernelCredentialRequestV1
            {
                AbiVersion = 1,
                StructSize = 56,
                ContextId = contextId,
                RequestId = requestId,
                CredentialKind = 1,
                TimeoutMs = 30000,
                MinimumLifetimeMs = 90000,
            };

        private static Dictionary<string, string> Storage(string endpoint)
            => new Dictionary<string, string>
            {
                ["account_name"] = "syntheticaccount",
                ["container_name"] = "container",
                ["azure_storage_endpoint"] = endpoint,
                ["allow_http"] = "true",
            };

        private static async Task WaitAsync(Task task)
        {
            Assert.Same(task, await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(10))));
            await task;
        }

        private static async Task DrainAsync(KernelCredentialRegistration registration)
        {
            Assert.True(await Task.Run(() => SpinWait.SpinUntil(() => registration.PendingRequestCount == 0, TimeSpan.FromSeconds(10))));
        }

        private static TaskCompletionSource<bool> Signal()
            => new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        private static TaskCompletionSource<AzureBearerToken> TokenSignal()
            => new TaskCompletionSource<AzureBearerToken>(TaskCreationOptions.RunContinuationsAsynchronously);

        private static AzureBearerToken Token()
            => new AzureBearerToken("synthetic.A-_/+~==", DateTimeOffset.UtcNow.AddMinutes(10));

        private static void Collect()
        {
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true);
            GC.WaitForPendingFinalizers();
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static (WeakReference Registration, Task Released, IntPtr Engine) CreateRetiredConsumer(IAzureBearerTokenProvider provider, string endpoint)
        {
            var registration = KernelCredentialRegistration.Create(new KernelAzureBearerCredentialOptions(provider));
            try
            {
                var engine = NewEngine(registration, "az://container/table", Storage(endpoint));
                var result = (new WeakReference(registration), registration.Released, engine);
                registration.Dispose();
                return result;
            }
            catch
            {
                registration.Dispose();
                throw;
            }
        }

        [Theory]
        [InlineData(typeof(KernelCredentialRequestV1), 56)]
        [InlineData(typeof(KernelCredentialResultV1), 56)]
        [InlineData(typeof(KernelCredentialRegistrationV1), 72)]
        [InlineData(typeof(KernelCredentialOptionV1), 32)]
        [InlineData(typeof(ExternResultHandleSharedExternEngine), 16)]
        [InlineData(typeof(ExternResultbool), 16)]
        [InlineData(typeof(CDvInfo), 16)]
        public void GivenAbiV1_WhenMeasuringStructures_MatchesNativeSize(Type type, int size)
        {
            Assert.Equal(8, IntPtr.Size);
            Assert.Equal(size, Marshal.SizeOf(type));
        }

        [Fact]
        public void GivenNativeBooleanPayload_WhenImported_PreservesOneByteValues()
        {
            var pointer = Marshal.AllocHGlobal(16);
            try
            {
                Marshal.WriteInt64(pointer, 0, 0);
                Marshal.WriteInt64(pointer, 8, 0);
                Assert.False(Marshal.PtrToStructure<ExternResultbool>(pointer).Anonymous.Anonymous1.ok);
                Assert.False(Marshal.PtrToStructure<CDvInfo>(pointer).has_vector);
                Marshal.WriteByte(pointer, 8, 1);
                Assert.True(Marshal.PtrToStructure<ExternResultbool>(pointer).Anonymous.Anonymous1.ok);
                Assert.True(Marshal.PtrToStructure<CDvInfo>(pointer).has_vector);
            }
            finally
            {
                Marshal.FreeHGlobal(pointer);
            }
        }

        [Theory]
        [InlineData(typeof(KernelCredentialRequestV1))]
        [InlineData(typeof(KernelCredentialResultV1))]
        [InlineData(typeof(KernelCredentialRegistrationV1))]
        [InlineData(typeof(KernelCredentialOptionV1))]
        public void GivenGeneratedCredentialHeader_WhenFieldsAreCompared_ManagedOrderMatches(Type type)
        {
            using var resource = typeof(KernelCredentialRegistrationTests).Assembly.GetManifestResourceStream(
                "DeltaLake.Tests.Kernel.delta_dotnet_kernel_credentials.h");
            Assert.NotNull(resource);
            using var reader = new System.IO.StreamReader(resource!);
            var header = reader.ReadToEnd();
            var body = Regex.Match(header, @"typedef struct " + type.Name + @"\s*\{(?<fields>[\s\S]*?)\}\s*" + type.Name + @";");
            Assert.True(body.Success);
            var nativeFields = new List<string>();
            foreach (var declaration in body.Groups["fields"].Value.Split(';'))
            {
                var field = Regex.Match(declaration.Trim(), @"(?<name>\w+)\s*(?:\[2\])?$");
                if (!field.Success) field = Regex.Match(declaration.Trim(), @"\(\*(?<name>\w+)\)");
                if (!field.Success) continue;
                var name = field.Groups["name"].Value.Replace("_", string.Empty);
                if (name == "reserved")
                {
                    nativeFields.Add("reserved0");
                    nativeFields.Add("reserved1");
                }
                else
                {
                    nativeFields.Add(name);
                }
            }
            var managedFields = type.GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
                .OrderBy(field => Marshal.OffsetOf(type, field.Name).ToInt64())
                .Select(field => field.Name.ToLowerInvariant()).ToArray();
            Assert.Equal(nativeFields, managedFields);
        }

        [Theory]
        [InlineData(typeof(KernelCredentialRequestV1), "ContextId", 8)]
        [InlineData(typeof(KernelCredentialRequestV1), "RequestId", 16)]
        [InlineData(typeof(KernelCredentialRequestV1), "CredentialKind", 24)]
        [InlineData(typeof(KernelCredentialRequestV1), "TimeoutMs", 28)]
        [InlineData(typeof(KernelCredentialRequestV1), "MinimumLifetimeMs", 32)]
        [InlineData(typeof(KernelCredentialRequestV1), "Flags", 36)]
        [InlineData(typeof(KernelCredentialRequestV1), "Reserved0", 40)]
        [InlineData(typeof(KernelCredentialRequestV1), "Reserved1", 48)]
        [InlineData(typeof(KernelCredentialResultV1), "Status", 8)]
        [InlineData(typeof(KernelCredentialResultV1), "CredentialKind", 12)]
        [InlineData(typeof(KernelCredentialResultV1), "TokenUtf8", 16)]
        [InlineData(typeof(KernelCredentialResultV1), "TokenLen", 24)]
        [InlineData(typeof(KernelCredentialResultV1), "ExpiresUnixMs", 32)]
        [InlineData(typeof(KernelCredentialResultV1), "Reserved0", 40)]
        [InlineData(typeof(KernelCredentialResultV1), "Reserved1", 48)]
        [InlineData(typeof(KernelCredentialRegistrationV1), "ContextId", 8)]
        [InlineData(typeof(KernelCredentialRegistrationV1), "CredentialKind", 16)]
        [InlineData(typeof(KernelCredentialRegistrationV1), "AcquisitionTimeoutMs", 20)]
        [InlineData(typeof(KernelCredentialRegistrationV1), "MaxTokenBytes", 24)]
        [InlineData(typeof(KernelCredentialRegistrationV1), "Flags", 28)]
        [InlineData(typeof(KernelCredentialRegistrationV1), "Begin", 32)]
        [InlineData(typeof(KernelCredentialRegistrationV1), "Cancel", 40)]
        [InlineData(typeof(KernelCredentialRegistrationV1), "Released", 48)]
        [InlineData(typeof(KernelCredentialRegistrationV1), "Reserved0", 56)]
        [InlineData(typeof(KernelCredentialRegistrationV1), "Reserved1", 64)]
        [InlineData(typeof(KernelCredentialOptionV1), "KeyUtf8", 0)]
        [InlineData(typeof(KernelCredentialOptionV1), "KeyLen", 8)]
        [InlineData(typeof(KernelCredentialOptionV1), "ValueUtf8", 16)]
        [InlineData(typeof(KernelCredentialOptionV1), "ValueLen", 24)]
        [InlineData(typeof(ExternResultHandleSharedExternEngine), "Anonymous", 8)]
        public void GivenAbiV1_WhenMeasuringFields_MatchesNativeOffset(Type type, string field, int offset)
            => Assert.Equal((long)offset, Marshal.OffsetOf(type, field).ToInt64());

        [Fact]
        public void GivenAbiV1_WhenInspectingImports_UsesExistingImageAndCdecl()
        {
            var imports = typeof(KernelCredentialInterop).GetMethods(BindingFlags.Static | BindingFlags.NonPublic);
            Assert.Equal(7, imports.Length);
            foreach (var method in imports)
            {
                var attribute = method.GetCustomAttribute<DllImportAttribute>()!;
                Assert.Equal("delta_kernel_ffi", attribute.Value);
                Assert.Equal(CallingConvention.Cdecl, attribute.CallingConvention);
                Assert.True(attribute.ExactSpelling);
            }

            foreach (var type in new[] { typeof(KernelCredentialBegin), typeof(KernelCredentialCancel), typeof(KernelCredentialReleased), typeof(AllocateErrorFn) })
            {
                Assert.Equal(CallingConvention.Cdecl, type.GetCustomAttribute<UnmanagedFunctionPointerAttribute>()!.CallingConvention);
            }
        }

        [Fact]
        public void GivenMissingOrSupportedHost_WhenPreflighting_ReturnsOrThrowsSanitizedError()
        {
            try
            {
                KernelCredentialRegistration.EnsureSupported();
                Assert.Equal((uint)1, KernelCredentialInterop.kernel_credential_abi_version());
            }
            catch (NotSupportedException error)
            {
                Assert.Equal("The loaded kernel library does not support credential ABI v1.", error.Message);
                Assert.Null(error.InnerException);
            }
        }

        [Fact]
        public void GivenCapacityExhaustion_WhenClassifyingRequestFailure_ReturnsNativeTransientStatus()
            => Assert.Equal(2u, KernelCredentialRequest.GetFailureStatus(new KernelCredentialCapacityException()));

        [Theory]
        [InlineData("generic")]
        [InlineData("invalid-operation")]
        [InlineData("capacity-message")]
        [InlineData("wrapped-capacity")]
        public void GivenOtherFailure_WhenClassifyingRequestFailure_ReturnsNativePermanentStatus(string failure)
        {
            var error = failure switch
            {
                "invalid-operation" => new InvalidOperationException("synthetic-secret"),
                "capacity-message" => new InvalidOperationException("Credential provider execution capacity is exhausted."),
                "wrapped-capacity" => new Exception("synthetic-secret", new KernelCredentialCapacityException()),
                _ => new Exception("synthetic-secret"),
            };

            Assert.Equal(1u, KernelCredentialRequest.GetFailureStatus(error));
        }

        [Fact]
        public void GivenNullOptions_WhenRegistering_RejectsBeforeNativeCall()
            => Assert.Throws<ArgumentNullException>(() => KernelCredentialRegistration.Create(null!));

        [Fact]
        public void GivenNullRequest_WhenBeginning_RejectsWithoutDereferencing()
            => Assert.Equal((uint)3, BeginRequest(IntPtr.Zero));

        [Theory]
        [InlineData(2, 56)]
        [InlineData(1, 8)]
        public void GivenUnsupportedHeader_WhenBeginning_RejectsBeforeReadingPayload(int version, int size)
        {
            var pointer = Marshal.AllocHGlobal(8);
            try
            {
                Marshal.WriteInt32(pointer, 0, version);
                Marshal.WriteInt32(pointer, 4, size);
                Assert.Equal((uint)3, BeginRequest(pointer));
            }
            finally
            {
                Marshal.FreeHGlobal(pointer);
            }
        }

        [Theory]
        [InlineData("context")]
        [InlineData("request")]
        [InlineData("kind")]
        [InlineData("timeout-zero")]
        [InlineData("timeout-large")]
        [InlineData("lifetime")]
        [InlineData("flags")]
        [InlineData("reserved-zero")]
        [InlineData("reserved-one")]
        public void GivenMalformedRequest_WhenBeginning_ReturnsInvalidRequest(string field)
        {
            var request = Request(ulong.MaxValue);
            switch (field)
            {
                case "context": request.ContextId = 0; break;
                case "request": request.RequestId = 0; break;
                case "kind": request.CredentialKind = 2; break;
                case "timeout-zero": request.TimeoutMs = 0; break;
                case "timeout-large": request.TimeoutMs = 120001; break;
                case "lifetime": request.MinimumLifetimeMs = uint.MaxValue; break;
                case "flags": request.Flags = 1; break;
                case "reserved-zero": request.Reserved0 = 1; break;
                case "reserved-one": request.Reserved1 = 1; break;
            }

            Assert.Equal((uint)3, Begin(request));
        }

        [Fact]
        public void GivenUnknownContext_WhenBeginning_ReturnsClosing()
            => Assert.Equal((uint)2, Begin(Request(ulong.MaxValue)));

        [KernelCredentialHostFact]
        public async Task GivenDormantRegistration_WhenActivatedAndDisposed_ReleasesExactlyOnceWithoutOwningProvider()
        {
            var provider = new TrackingProvider();
            var baseline = KernelCredentialRegistration.RegistrationCount;
            var registration = KernelCredentialRegistration.Create(new KernelAzureBearerCredentialOptions(provider));
            Assert.NotEqual((ulong)0, registration.ContextId);
            Assert.Equal(baseline + 1, KernelCredentialRegistration.RegistrationCount);
            Assert.False(registration.Released.IsCompleted);

            registration.Dispose();
            registration.Dispose();
            await WaitAsync(registration.Released);

            Assert.Equal(baseline, KernelCredentialRegistration.RegistrationCount);
            Assert.Equal(0, provider.DisposeCount);
            Assert.Equal(0, provider.CallCount);
            Assert.Equal((uint)2, Begin(Request(registration.ContextId)));
            Assert.Throws<ObjectDisposedException>(() => NewEngine(registration, "az://container/table", Storage("http://127.0.0.1:1")));
        }

        [KernelCredentialHostFact]
        public async Task GivenNativeEngine_WhenUnregistering_RootSurvivesGcAndConstructionPerformsNoAcquisitionOrHttp()
        {
            var provider = new TrackingProvider();
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var endpoint = "http://127.0.0.1:" + ((IPEndPoint)listener.LocalEndpoint).Port;
            var consumer = CreateRetiredConsumer(provider, endpoint);
            try
            {
                Collect();
                Assert.True(consumer.Registration.IsAlive);
                Assert.False(consumer.Released.IsCompleted);
                Assert.NotEqual(IntPtr.Zero, consumer.Engine);
                Assert.Equal(0, provider.CallCount);
                Assert.False(listener.Pending());
            }
            finally
            {
                free_engine(consumer.Engine);
                listener.Stop();
            }

            await WaitAsync(consumer.Released);
            Collect();
            Assert.False(consumer.Registration.IsAlive);
            Assert.Equal(0, provider.DisposeCount);
        }

        [KernelCredentialHostFact]
        public async Task GivenNativeConstructorFailure_WhenHandlingError_UsesSanitizedOwnedErrorAfterOtherRegistrationReleased()
        {
            var provider = new TrackingProvider();
            var previous = KernelCredentialRegistration.Create(new KernelAzureBearerCredentialOptions(provider));
            previous.Dispose();
            await WaitAsync(previous.Released);
            Collect();

            using var registration = KernelCredentialRegistration.Create(new KernelAzureBearerCredentialOptions(provider));
            var options = Storage("http://127.0.0.1:1");
            options["bearer_token"] = "synthetic-secret";
            var error = Assert.Throws<KernelException>(() => NewEngine(registration, "az://container/table", options));
            Assert.DoesNotContain("synthetic-secret", error.ToString());
            Assert.Contains("kernel credential host:", error.KernelMessage);
            Assert.Equal(0, provider.CallCount);
        }

        [KernelCredentialHostFact]
        public async Task GivenDuplicateAdmission_WhenProviderIsPending_PreservesOriginalRequestAndSuppressesAmbientContext()
        {
            var entered = Signal();
            var token = TokenSignal();
            string? captured = "unobserved";
            var provider = new DelegateAzureBearerTokenProvider((context, cancellation) =>
            {
                captured = Ambient.Value;
                Assert.Equal(TimeSpan.FromSeconds(90), context.MinimumLifetime);
                Assert.Equal("https://storage.azure.com/.default", Assert.Single(context.Scopes));
                entered.TrySetResult(true);
                return token.Task;
            });
            using var registration = KernelCredentialRegistration.Create(new KernelAzureBearerCredentialOptions(provider));
            Ambient.Value = "caller-context";
            try
            {
                Assert.Equal((uint)0, Begin(Request(registration.ContextId)));
                await WaitAsync(entered.Task);
                Assert.Equal((uint)3, Begin(Request(registration.ContextId)));
                Assert.Equal(1, registration.PendingRequestCount);
                Assert.Null(captured);
            }
            finally
            {
                Ambient.Value = null;
                token.TrySetResult(Token());
            }

            await DrainAsync(registration);
        }

        [KernelCredentialHostFact]
        public async Task GivenAcceptedRequest_WhenNativeReleasePrecedesProviderReturn_RetainsRequestAndAcceptsLateCompletion()
        {
            var entered = Signal();
            var token = TokenSignal();
            var provider = new DelegateAzureBearerTokenProvider((context, cancellation) =>
            {
                entered.TrySetResult(true);
                return token.Task;
            });
            var registration = KernelCredentialRegistration.Create(new KernelAzureBearerCredentialOptions(provider));
            try
            {
                Assert.Equal((uint)0, Begin(Request(registration.ContextId)));
                await WaitAsync(entered.Task);
                registration.Dispose();
                await WaitAsync(registration.Released);
                Collect();
                Assert.Equal(1, registration.PendingRequestCount);
                Assert.Equal((uint)2, Begin(Request(registration.ContextId, ulong.MaxValue - 1)));
            }
            finally
            {
                token.TrySetResult(Token());
                registration.Dispose();
            }

            await DrainAsync(registration);
        }

        [KernelCredentialHostFact]
        public async Task GivenBlockingProviderCancellation_WhenCancelCallbackRuns_QueuesCancellationAndRetainsActualWork()
        {
            var entered = Signal();
            var canceled = Signal();
            var allowCancel = Signal();
            var token = TokenSignal();
            var provider = new DelegateAzureBearerTokenProvider(async (context, cancellation) =>
            {
                using var cancellationRegistration = cancellation.Register(() =>
                {
                    canceled.TrySetResult(true);
                    allowCancel.Task.GetAwaiter().GetResult();
                });
                entered.TrySetResult(true);
                return await token.Task;
            });
            using var registration = KernelCredentialRegistration.Create(new KernelAzureBearerCredentialOptions(provider));
            try
            {
                Assert.Equal((uint)0, Begin(Request(registration.ContextId)));
                await WaitAsync(entered.Task);
                CancelRequest(registration.ContextId, ulong.MaxValue, 2);
                await WaitAsync(canceled.Task);
                Assert.Equal(1, registration.PendingRequestCount);
                registration.Dispose();
                await WaitAsync(registration.Released);
            }
            finally
            {
                allowCancel.TrySetResult(true);
                token.TrySetResult(Token());
            }

            await DrainAsync(registration);
        }

        [KernelCredentialHostFact]
        public async Task GivenExistingConsumer_WhenBeginningAfterDispose_ContinuesAcquisitionUntilNativeRelease()
        {
            var entered = Signal();
            var token = TokenSignal();
            var provider = new DelegateAzureBearerTokenProvider((context, cancellation) =>
            {
                entered.TrySetResult(true);
                return token.Task;
            });
            using var registration = KernelCredentialRegistration.Create(new KernelAzureBearerCredentialOptions(provider));
            var engine = NewEngine(registration, "az://container/table", Storage("http://127.0.0.1:1"));
            try
            {
                registration.Dispose();
                Assert.False(registration.Released.IsCompleted);
                Assert.Equal((uint)0, Begin(Request(registration.ContextId)));
                await WaitAsync(entered.Task);
                Assert.Equal(1, registration.PendingRequestCount);
            }
            finally
            {
                free_engine(engine);
                token.TrySetResult(Token());
            }

            await WaitAsync(registration.Released);
            await DrainAsync(registration);
        }

        [KernelCredentialHostFact]
        public async Task GivenUncooperativeProviders_WhenCancelingRequests_RetainsActualTaskCapacityUntilTheyFinish()
        {
            var entered = Signal();
            var token = TokenSignal();
            var calls = 0;
            var provider = new DelegateAzureBearerTokenProvider((context, cancellation) =>
            {
                if (Interlocked.Increment(ref calls) == 64)
                {
                    entered.TrySetResult(true);
                }

                return token.Task;
            });
            using var registration = KernelCredentialRegistration.Create(new KernelAzureBearerCredentialOptions(provider));
            try
            {
                for (ulong requestId = 1; requestId <= 64; requestId++)
                {
                    Assert.Equal((uint)0, Begin(Request(registration.ContextId, requestId)));
                }

                await WaitAsync(entered.Task);
                for (ulong requestId = 1; requestId <= 64; requestId++)
                {
                    CancelRequest(registration.ContextId, requestId, 2);
                }

                Assert.Equal((uint)1, Begin(Request(registration.ContextId, 65)));
                Assert.Equal(64, registration.PendingRequestCount);
                Assert.Equal(64, Volatile.Read(ref calls));
                registration.Dispose();
                await WaitAsync(registration.Released);
                Assert.Equal(64, registration.PendingRequestCount);
            }
            finally
            {
                token.TrySetResult(Token());
            }

            await DrainAsync(registration);
        }

        [KernelCredentialHostFact]
        public void GivenInvalidManagedEngineBuffers_WhenConstructing_RejectsBoundsAndStrictUtf8WithoutEcho()
        {
            using var registration = KernelCredentialRegistration.Create(new KernelAzureBearerCredentialOptions(new TrackingProvider()));
            var invalidOptions = new[]
            {
                new Dictionary<string, string> { [new string('k', 1025)] = "value" },
                new Dictionary<string, string> { ["key"] = new string('v', 65537) },
                new Dictionary<string, string> { ["key"] = "secret\0value" },
                new Dictionary<string, string> { ["key"] = "secret\uD800value" },
                new Dictionary<string, string> { [new string('\u00E9', 513)] = "value" },
            };
            foreach (var options in invalidOptions)
            {
                var error = Assert.Throws<ArgumentException>(() => NewEngine(registration, "az://container/table", options));
                Assert.DoesNotContain("secret", error.ToString());
            }

            var tooMany = new Dictionary<string, string>();
            for (var index = 0; index < 257; index++)
            {
                tooMany.Add("key" + index, "value");
            }

            Assert.Throws<ArgumentException>(() => NewEngine(registration, "az://container/table", tooMany));
            var tooLarge = new Dictionary<string, string>();
            for (var index = 0; index < 17; index++)
            {
                tooLarge.Add("key" + index, new string('v', 65536));
            }

            Assert.Throws<ArgumentException>(() => NewEngine(registration, "az://container/table", tooLarge));
            Assert.Throws<ArgumentException>(() => NewEngine(registration, "az://container/secret\uD800", Storage("http://127.0.0.1:1")));
        }

        [KernelCredentialHostFact]
        public void GivenMalformedRegistration_WhenRegistering_PublishesNothingAndCallsNoCallbacks()
        {
            var callbacks = 0;
            var begin = new ProbeBegin(request => { Interlocked.Increment(ref callbacks); return 4; });
            var cancel = new ProbeCancel((context, request, reason) => Interlocked.Increment(ref callbacks));
            var released = new ProbeReleased(context => Interlocked.Increment(ref callbacks));
            Assert.Equal((uint)0, KernelCredentialInterop.kernel_credential_context_new(out var contextId));
            var pointer = Marshal.AllocHGlobal(72);
            try
            {
                var descriptor = new KernelCredentialRegistrationV1
                {
                    AbiVersion = 1,
                    StructSize = 72,
                    ContextId = contextId,
                    CredentialKind = 1,
                    AcquisitionTimeoutMs = 30000,
                    MaxTokenBytes = 65536,
                    Begin = Marshal.GetFunctionPointerForDelegate(begin),
                    Cancel = Marshal.GetFunctionPointerForDelegate(cancel),
                    Released = Marshal.GetFunctionPointerForDelegate(released),
                    Reserved0 = 1,
                };
                Marshal.StructureToPtr(descriptor, pointer, false);
                Assert.Equal((uint)1, RegisterDescriptor(pointer));
                Assert.Equal((uint)2, KernelCredentialInterop.kernel_credential_activate(contextId));
            }
            finally
            {
                Marshal.FreeHGlobal(pointer);
                Assert.Equal((uint)0, KernelCredentialInterop.kernel_credential_unregister(contextId));
                GC.KeepAlive(begin);
                GC.KeepAlive(cancel);
                GC.KeepAlive(released);
            }

            Assert.Equal(0, callbacks);
        }

        private sealed class TrackingProvider : IAzureBearerTokenProvider, IDisposable
        {
            internal int CallCount;
            internal int DisposeCount;

            public Task<AzureBearerToken> GetTokenAsync(AzureTokenRequestContext context, CancellationToken cancellationToken)
            {
                Interlocked.Increment(ref CallCount);
                return Task.FromResult(Token());
            }

            public void Dispose() => Interlocked.Increment(ref DisposeCount);
        }
    }

    internal sealed class KernelCredentialHostFactAttribute : FactAttribute
    {
        public KernelCredentialHostFactAttribute()
        {
            try
            {
                KernelCredentialRegistration.EnsureSupported();
            }
            catch (NotSupportedException)
            {
                Skip = "Credential ABI v1 host must be packaged as the existing delta_kernel_ffi library.";
            }
        }
    }
}