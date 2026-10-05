using Apache.Arrow;
using DeltaLake.Credentials;
using DeltaLake.Kernel.Credentials;
using DeltaLake.Table;

namespace DeltaLake.Tests.Table
{
    [Collection("KernelCredentialContract")]
    public class KernelCredentialContractTests
    {
        [Fact]
        public void GivenOffsetExpiry_WhenCreatingToken_NormalizesToUtc()
        {
            var expiry = new DateTimeOffset(2026, 10, 2, 12, 0, 0, TimeSpan.FromHours(2));
            var token = new AzureBearerToken("synthetic-secret", expiry);

            Assert.Equal("synthetic-secret", token.Token);
            Assert.Equal(expiry, token.ExpiresOn);
            Assert.Equal(TimeSpan.Zero, token.ExpiresOn.Offset);
        }

        [Fact]
        public void GivenSecretToken_WhenFormatting_RedactsContents()
        {
            var token = new AzureBearerToken("synthetic-secret", DateTimeOffset.UtcNow);

            Assert.DoesNotContain(token.Token, token.ToString());
            Assert.Contains("[REDACTED]", token.ToString());
        }

        [Fact]
        public void GivenNullToken_WhenCreatingToken_RejectsReference()
        {
            Assert.Throws<ArgumentNullException>(() => new AzureBearerToken(null!, DateTimeOffset.UtcNow));
        }

        [Fact]
        public void GivenMutableScopes_WhenCreatingOptionsAndContext_CopiesReadOnlyValues()
        {
            var scopes = new List<string> { "https://storage.azure.com/.default" };
            var options = new KernelAzureBearerCredentialOptions(
                new DelegateAzureBearerTokenProvider((context, cancellation) => Task.FromResult(new AzureBearerToken("token", DateTimeOffset.MaxValue))), scopes);
            var context = new AzureTokenRequestContext(scopes, TimeSpan.FromSeconds(90));
            scopes.Clear();

            Assert.Equal("https://storage.azure.com/.default", Assert.Single(options.Scopes));
            Assert.Equal(options.Scopes, context.Scopes);
            Assert.Throws<NotSupportedException>(() => ((IList<string>)options.Scopes).Add("https://other.example/.default"));
            Assert.Throws<NotSupportedException>(() => ((IList<string>)context.Scopes).Clear());
        }

        [Fact]
        public void GivenDefaultOptions_WhenConstructing_UsesContractDefaultsAndRedacts()
        {
            var provider = new DelegateAzureBearerTokenProvider((context, cancellation) => Task.FromResult(new AzureBearerToken("token", DateTimeOffset.MaxValue)));
            var options = new KernelAzureBearerCredentialOptions(provider);

            Assert.Same(provider, options.Provider);
            Assert.Equal(TimeSpan.FromSeconds(30), options.AcquisitionTimeout);
            Assert.Equal(65536, options.MaxTokenBytes);
            Assert.Equal("https://storage.azure.com/.default", Assert.Single(options.Scopes));
            Assert.DoesNotContain(options.Scopes[0], options.ToString());
            Assert.Contains("[REDACTED]", options.ToString());
        }

        [Theory]
        [InlineData(0)]
        [InlineData(999)]
        [InlineData(120001)]
        public void GivenInvalidTimeout_WhenConstructing_RejectsBounds(int milliseconds)
        {
            var provider = new DelegateAzureBearerTokenProvider((context, cancellation) => Task.FromResult(new AzureBearerToken("token", DateTimeOffset.MaxValue)));
            Assert.Throws<ArgumentOutOfRangeException>(() => new KernelAzureBearerCredentialOptions(provider, acquisitionTimeout: TimeSpan.FromMilliseconds(milliseconds)));
        }

        [Fact]
        public void GivenFractionalMillisecond_WhenConstructing_RejectsTruncation()
        {
            var provider = new DelegateAzureBearerTokenProvider((context, cancellation) => Task.FromResult(new AzureBearerToken("token", DateTimeOffset.MaxValue)));
            Assert.Throws<ArgumentOutOfRangeException>(() => new KernelAzureBearerCredentialOptions(provider, acquisitionTimeout: TimeSpan.FromSeconds(1) + TimeSpan.FromTicks(1)));
        }

        [Theory]
        [InlineData(1, 1)]
        [InlineData(120, 65536)]
        public void GivenBoundaryLimits_WhenConstructing_AcceptsValues(int seconds, int bytes)
        {
            var provider = new DelegateAzureBearerTokenProvider((context, cancellation) => Task.FromResult(new AzureBearerToken("token", DateTimeOffset.MaxValue)));
            var options = new KernelAzureBearerCredentialOptions(provider, acquisitionTimeout: TimeSpan.FromSeconds(seconds), maxTokenBytes: bytes);
            Assert.Equal(bytes, options.MaxTokenBytes);
            Assert.Equal(TimeSpan.FromSeconds(seconds), options.AcquisitionTimeout);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(-1)]
        [InlineData(65537)]
        public void GivenInvalidTokenLimit_WhenConstructing_RejectsBounds(int bytes)
        {
            var provider = new DelegateAzureBearerTokenProvider((context, cancellation) => Task.FromResult(new AzureBearerToken("token", DateTimeOffset.MaxValue)));
            Assert.Throws<ArgumentOutOfRangeException>(() => new KernelAzureBearerCredentialOptions(provider, maxTokenBytes: bytes));
        }

        [Theory]
        [InlineData("")]
        [InlineData(" ")]
        [InlineData("http://storage.azure.com/.default")]
        [InlineData("https://storage.azure.com/.default?secret")]
        [InlineData("https://secret@storage.azure.com/.default")]
        [InlineData("https://storage.azure.com/other")]
        public void GivenInvalidScope_WhenConstructing_RejectsWithoutEcho(string scope)
        {
            var provider = new DelegateAzureBearerTokenProvider((context, cancellation) => Task.FromResult(new AzureBearerToken("token", DateTimeOffset.MaxValue)));
            var error = Assert.Throws<ArgumentException>(() => new KernelAzureBearerCredentialOptions(provider, new[] { scope }));
            Assert.Equal("scopes", error.ParamName);
            Assert.Null(error.InnerException);
        }

        [Fact]
        public void GivenMissingProviderOrScopes_WhenConstructing_RejectsStructuralErrors()
        {
            var provider = new DelegateAzureBearerTokenProvider((context, cancellation) => Task.FromResult(new AzureBearerToken("token", DateTimeOffset.MaxValue)));
            Assert.Throws<ArgumentNullException>(() => new DelegateAzureBearerTokenProvider(null!));
            Assert.Throws<ArgumentNullException>(() => new KernelAzureBearerCredentialOptions(null!));
            Assert.Throws<ArgumentException>(() => new KernelAzureBearerCredentialOptions(provider, System.Array.Empty<string>()));
            Assert.Throws<ArgumentException>(() => new KernelAzureBearerCredentialOptions(provider, new[] { (string)null! }));
        }

        [Fact]
        public void GivenMutableLoadOptions_WhenSnapshotting_CapturesScalarsAndPrivateStorage()
        {
            var options = new TableOptions
            {
                TableLocation = "az://container/table",
                Version = 7,
                WithoutFiles = true,
                LogBufferSize = 11,
                StorageOptions = new Dictionary<string, string> { ["account_name"] = "original" },
            };
            var snapshot = KernelCredentialBootstrap.Snapshot(options);
            options.Version = 99;
            options.WithoutFiles = false;
            options.LogBufferSize = 100;
            options.StorageOptions["account_name"] = "mutated";
            var privateOptions = KernelCredentialBootstrap.WithToken(snapshot, new AzureBearerToken("token", DateTimeOffset.MaxValue));
            privateOptions.StorageOptions["account_name"] = "private";

            Assert.Equal((ulong)7, snapshot.Version);
            Assert.True(snapshot.WithoutFiles);
            Assert.Equal((uint)11, snapshot.LogBufferSize);
            Assert.Equal("original", snapshot.StorageOptions["account_name"]);
            Assert.Equal("token", privateOptions.StorageOptions["bearer_token"]);
            Assert.False(snapshot.StorageOptions.ContainsKey("bearer_token"));
            Assert.False(options.StorageOptions.ContainsKey("bearer_token"));
        }

        [Fact]
        public void GivenMutableCreateOptions_WhenSnapshotting_CapturesCollectionsAndMetadata()
        {
            var metadata = new Dictionary<string, string> { ["schema"] = "original" };
            var options = new TableCreateOptions("az://container/table", new Schema(System.Array.Empty<Field>(), metadata))
            {
                PartitionBy = new List<string> { "partition" },
                SaveMode = SaveMode.Append,
                Name = "original",
                Description = "original description",
                StorageOptions = new Dictionary<string, string> { ["account_name"] = "original" },
                Configuration = new Dictionary<string, string> { ["config"] = "original" },
                CustomMetadata = new Dictionary<string, string> { ["custom"] = "original" },
            };
            var snapshot = KernelCredentialBootstrap.Snapshot(options);
            options.PartitionBy.Clear();
            options.Name = "mutated";
            options.Description = "mutated";
            options.SaveMode = SaveMode.Overwrite;
            options.StorageOptions.Clear();
            options.Configuration.Clear();
            options.CustomMetadata.Clear();
            metadata.Clear();
            var privateOptions = KernelCredentialBootstrap.WithToken(snapshot, new AzureBearerToken("token", DateTimeOffset.MaxValue));
            privateOptions.Configuration!["config"] = "private";
            privateOptions.CustomMetadata!["custom"] = "private";

            Assert.Equal("partition", Assert.Single(snapshot.PartitionBy));
            Assert.Equal("original", snapshot.Name);
            Assert.Equal("original description", snapshot.Description);
            Assert.Equal(SaveMode.Append, snapshot.SaveMode);
            Assert.Equal("original", snapshot.Configuration!["config"]);
            Assert.Equal("original", snapshot.CustomMetadata!["custom"]);
            Assert.Equal("original", snapshot.Schema.Metadata["schema"]);
            Assert.Equal("original", snapshot.StorageOptions["account_name"]);
            Assert.False(snapshot.StorageOptions.ContainsKey("bearer_token"));
            Assert.False(options.StorageOptions.ContainsKey("bearer_token"));
            Assert.Equal("token", privateOptions.StorageOptions["bearer_token"]);
        }

        [Theory]
        [InlineData("")]
        [InlineData("=")]
        [InlineData("===")]
        [InlineData("a=b")]
        [InlineData("a b")]
        [InlineData("a\r\nb")]
        [InlineData("a\0b")]
        [InlineData("a\u00e9b")]
        [InlineData("a:b")]
        public void GivenInvalidTokenBytes_WhenValidating_RejectsWithoutSecret(string value)
        {
            var error = Assert.Throws<InvalidOperationException>(() => KernelCredentialBootstrap.ValidateToken(new AzureBearerToken(value, DateTimeOffset.MaxValue), 65536));
            Assert.Equal("The credential provider returned an unusable token.", error.Message);
            Assert.Null(error.InnerException);
        }

        [Theory]
        [InlineData("a")]
        [InlineData("AZaz09-._~+/==")]
        public void GivenBearerAlphabet_WhenValidating_AcceptsContentAndTrailingPadding(string value)
        {
            KernelCredentialBootstrap.ValidateToken(new AzureBearerToken(value, DateTimeOffset.MaxValue), value.Length);
        }

        [Theory]
        [InlineData(-1)]
        [InlineData(0)]
        [InlineData(89)]
        [InlineData(90)]
        public void GivenInsufficientExpiry_WhenValidating_RejectsUsefulLifetime(int seconds)
        {
            var token = new AzureBearerToken("token", DateTimeOffset.UtcNow.AddSeconds(seconds));
            Assert.Throws<InvalidOperationException>(() => KernelCredentialBootstrap.ValidateToken(token, 65536));
        }

        [Fact]
        public void GivenOverLimitToken_WhenValidating_RejectsConfiguredCap()
        {
            Assert.Throws<InvalidOperationException>(() => KernelCredentialBootstrap.ValidateToken(new AzureBearerToken("abcd", DateTimeOffset.MaxValue), 3));
            Assert.Throws<InvalidOperationException>(() => KernelCredentialBootstrap.ValidateToken(new AzureBearerToken(new string('a', 65537), DateTimeOffset.MaxValue), 65536));
        }

        [Theory]
        [InlineData("az")]
        [InlineData("adl")]
        [InlineData("azure")]
        [InlineData("abfs")]
        [InlineData("abfss")]
        public void GivenApprovedScheme_WhenValidatingStorage_AcceptsExplicitAccount(string scheme)
        {
            var options = new TableOptions
            {
                TableLocation = scheme + "://container/table",
                StorageOptions = new Dictionary<string, string> { ["AZURE_STORAGE_ACCOUNT_NAME"] = "account" },
            };
            KernelCredentialBootstrap.ValidateStorage(options, new Dictionary<string, string>());
        }

        [Theory]
        [InlineData("file:///tmp/table")]
        [InlineData("memory://table")]
        [InlineData("s3://bucket/table")]
        [InlineData("https://account.blob.core.windows.net/container/table")]
        [InlineData("az://container/table?sig=secret")]
        [InlineData("abfs://container:secret@account.dfs.core.windows.net/table")]
        public void GivenUnsupportedLocation_WhenValidatingStorage_RejectsWithoutValues(string location)
        {
            var options = new TableOptions { TableLocation = location };
            var error = Assert.Throws<InvalidOperationException>(() => KernelCredentialBootstrap.ValidateStorage(options, new Dictionary<string, string>()));
            Assert.DoesNotContain(location, error.Message);
            Assert.Null(error.InnerException);
        }

        [Theory]
        [InlineData("account_key")]
        [InlineData("AZURE_STORAGE_ACCESS_KEY")]
        [InlineData("azure_storage_master_key")]
        [InlineData("sas_key")]
        [InlineData("AZURE_STORAGE_SAS_TOKEN")]
        [InlineData("bearer_token")]
        [InlineData("TOKEN")]
        [InlineData("azure_storage_token")]
        [InlineData("client_id")]
        [InlineData("azure_client_secret")]
        [InlineData("azure_storage_authority_id")]
        [InlineData("azure_authority_host")]
        [InlineData("azure_identity_endpoint")]
        [InlineData("object_id")]
        [InlineData("azure_msi_resource_id")]
        [InlineData("federated_token_file")]
        [InlineData("FABRIC_TOKEN_SERVICE_URL")]
        [InlineData("azure_fabric_session_token")]
        [InlineData("use_emulator")]
        [InlineData("object_store_use_emulator")]
        [InlineData("AZURE_SKIP_SIGNATURE")]
        [InlineData("use_azure_cli")]
        public void GivenCompetingAuth_WhenValidatingStorage_RejectsAliasWithoutSecret(string key)
        {
            var options = new TableOptions
            {
                TableLocation = "az://container/table",
                StorageOptions = new Dictionary<string, string> { ["account_name"] = "account", [key] = "secret-value" },
            };
            var error = Assert.Throws<InvalidOperationException>(() => KernelCredentialBootstrap.ValidateStorage(options, new Dictionary<string, string>()));
            Assert.DoesNotContain("secret-value", error.ToString());
            Assert.Null(error.InnerException);
        }

        [Fact]
        public void GivenDisabledBypassAndNonAuthOptions_WhenValidatingStorage_PreservesConfiguration()
        {
            var options = new TableOptions
            {
                TableLocation = "az://container/table",
                StorageOptions = new Dictionary<string, string>
                {
                    ["account_name"] = "account",
                    ["container_name"] = "container",
                    ["endpoint"] = "http://127.0.0.1:12345",
                    ["azure_allow_http"] = "true",
                    ["use_emulator"] = "false",
                    ["skip_signature"] = "false",
                    ["use_azure_cli"] = "false",
                    ["use_fabric_endpoint"] = "true",
                    ["timeout"] = "20s",
                },
            };
            var snapshot = KernelCredentialBootstrap.Snapshot(options);
            KernelCredentialBootstrap.ValidateStorage(snapshot, new Dictionary<string, string>());
            Assert.Equal(options.StorageOptions, snapshot.StorageOptions);
        }

        [Theory]
        [InlineData("AZURE_STORAGE_USE_EMULATOR")]
        [InlineData("azure_skip_signature")]
        [InlineData("AZURE_USE_AZURE_CLI")]
        [InlineData("AZURE_FABRIC_TOKEN_SERVICE_URL")]
        [InlineData("AZURE_FABRIC_SESSION_TOKEN")]
        public void GivenEffectiveAmbientAuth_WhenValidatingStorage_RejectsWithoutChangingEnvironment(string key)
        {
            var options = new TableOptions
            {
                TableLocation = "az://container/table",
                StorageOptions = new Dictionary<string, string> { ["account_name"] = "account" },
            };
            var environment = new Dictionary<string, string> { [key] = "secret-value" };
            var error = Assert.Throws<InvalidOperationException>(() => KernelCredentialBootstrap.ValidateStorage(options, environment));
            Assert.DoesNotContain("secret-value", error.ToString());
            Assert.Equal("secret-value", environment[key]);
        }

        [Fact]
        public void GivenExplicitDisabledBypass_WhenValidatingStorage_OverridesAmbientEnabledAlias()
        {
            var options = new TableOptions
            {
                TableLocation = "az://container/table",
                StorageOptions = new Dictionary<string, string> { ["account_name"] = "account", ["skip_signature"] = "false" },
            };
            KernelCredentialBootstrap.ValidateStorage(options, new Dictionary<string, string> { ["AZURE_SKIP_SIGNATURE"] = "true" });
        }

        [Fact]
        public void GivenOnlyAmbientAccount_WhenValidatingStorage_DoesNotImportKernelIdentity()
        {
            var options = new TableOptions { TableLocation = "az://container/table" };
            Assert.Throws<InvalidOperationException>(() => KernelCredentialBootstrap.ValidateStorage(options, new Dictionary<string, string> { ["AZURE_STORAGE_ACCOUNT_NAME"] = "account" }));
        }

        [Theory]
        [InlineData("abfs://container@account.dfs.core.windows.net/table")]
        [InlineData("abfss://container@account.blob.core.windows.net/table")]
        [InlineData("az://container@onelake.dfs.fabric.microsoft.com/table")]
        public void GivenEmbeddedAccount_WhenValidatingStorage_DoesNotRequireAmbientAccount(string location)
        {
            KernelCredentialBootstrap.ValidateStorage(new TableOptions { TableLocation = location }, new Dictionary<string, string>());
        }

        [Theory]
        [InlineData("http://127.0.0.1:12345")]
        [InlineData("http://dev.example:12345")]
        [InlineData("https://secret@account.example")]
        [InlineData("https://account.example?sig=secret")]
        public void GivenUnsafeEndpoint_WhenValidatingStorage_RequiresExplicitSafeConfiguration(string endpoint)
        {
            var options = new TableOptions
            {
                TableLocation = "az://container/table",
                StorageOptions = new Dictionary<string, string> { ["account_name"] = "account", ["endpoint"] = endpoint },
            };
            var error = Assert.Throws<InvalidOperationException>(() => KernelCredentialBootstrap.ValidateStorage(options, new Dictionary<string, string> { ["AZURE_ALLOW_HTTP"] = "true" }));
            Assert.DoesNotContain(endpoint, error.ToString());
        }

        [Fact]
        public void GivenConflictingTypedAliases_WhenValidatingStorage_RejectsAmbiguousPrecedence()
        {
            var options = new TableOptions
            {
                TableLocation = "az://container/table",
                StorageOptions = new Dictionary<string, string> { ["account_name"] = "account", ["AZURE_STORAGE_ACCOUNT_NAME"] = "different" },
            };
            Assert.Throws<InvalidOperationException>(() => KernelCredentialBootstrap.ValidateStorage(options, new Dictionary<string, string>()));
        }

        [Fact]
        public async Task GivenValidProvider_WhenAcquiring_QueuesOffCallerAndSuppliesImmutableContext()
        {
            var callerThread = 0;
            var providerThread = 0;
            AzureTokenRequestContext? observed = null;
            var provider = new DelegateAzureBearerTokenProvider((context, cancellation) =>
            {
                providerThread = Environment.CurrentManagedThreadId;
                observed = context;
                return Task.FromResult(new AzureBearerToken("token", DateTimeOffset.MaxValue));
            });
            var acquisition = new TaskCompletionSource<Task<AzureBearerToken>>(TaskCreationOptions.RunContinuationsAsynchronously);
            var thread = new Thread(() =>
            {
                callerThread = Environment.CurrentManagedThreadId;
                acquisition.SetResult(KernelCredentialBootstrap.AcquireAsync(new KernelAzureBearerCredentialOptions(provider), CancellationToken.None));
            });
            thread.Start();
            var token = await await WithinAsync(acquisition.Task);

            Assert.NotEqual(callerThread, providerThread);
            Assert.NotNull(observed);
            Assert.Equal(TimeSpan.FromSeconds(90), observed!.MinimumLifetime);
            Assert.Equal("https://storage.azure.com/.default", Assert.Single(observed.Scopes));
            Assert.Equal("token", token.Token);
        }

        [Theory]
        [InlineData("throw")]
        [InlineData("fault")]
        [InlineData("null-task")]
        [InlineData("null-token")]
        [InlineData("foreign-cancellation")]
        [InlineData("provider-timeout")]
        [InlineData("capacity-throw")]
        [InlineData("capacity-fault")]
        [InlineData("invalid-token")]
        public async Task GivenProviderFailure_WhenAcquiring_SanitizesWithoutInnerException(string failure)
        {
            var provider = new DelegateAzureBearerTokenProvider((context, cancellation) => failure switch
            {
                "throw" => throw new InvalidOperationException("synthetic-secret"),
                "fault" => Task.FromException<AzureBearerToken>(new Exception("synthetic-secret")),
                "null-task" => null!,
                "null-token" => Task.FromResult<AzureBearerToken>(null!),
                "foreign-cancellation" => Task.FromCanceled<AzureBearerToken>(new CancellationToken(true)),
                "provider-timeout" => Task.FromException<AzureBearerToken>(new TimeoutException("synthetic-secret")),
                "capacity-throw" => throw new KernelCredentialCapacityException(),
                "capacity-fault" => Task.FromException<AzureBearerToken>(new KernelCredentialCapacityException()),
                _ => Task.FromResult(new AzureBearerToken("invalid secret", DateTimeOffset.MaxValue)),
            });
            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => KernelCredentialBootstrap.AcquireAsync(new KernelAzureBearerCredentialOptions(provider), CancellationToken.None));
            Assert.Equal("The credential provider failed to acquire a usable token.", error.Message);
            Assert.DoesNotContain("synthetic-secret", error.ToString());
            Assert.Null(error.InnerException);
            Assert.Equal(1u, KernelCredentialRequest.GetFailureStatus(error));
        }

        [Fact]
        public async Task GivenAlreadyCanceledCaller_WhenAcquiring_DoesNotInvokeProvider()
        {
            var invoked = false;
            var provider = new DelegateAzureBearerTokenProvider((context, cancellation) =>
            {
                invoked = true;
                return Task.FromResult(new AzureBearerToken("token", DateTimeOffset.MaxValue));
            });
            var cancellation = new CancellationToken(true);
            var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => KernelCredentialBootstrap.AcquireAsync(new KernelAzureBearerCredentialOptions(provider), cancellation));
            Assert.Equal(cancellation, error.CancellationToken);
            Assert.False(invoked);
        }

        [Fact]
        public async Task GivenCallerCancellation_WhenProviderIgnoresIt_ReturnsCallerTokenAndKeepsSourceAlive()
        {
            using var callerCancellation = new CancellationTokenSource();
            var started = new TaskCompletionSource<CancellationToken>(TaskCreationOptions.RunContinuationsAsynchronously);
            var canceled = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var finish = new TaskCompletionSource<AzureBearerToken>();
            var provider = new DelegateAzureBearerTokenProvider((context, cancellation) =>
            {
                cancellation.Register(() => canceled.TrySetResult(true));
                started.SetResult(cancellation);
                return finish.Task;
            });
            var acquisition = KernelCredentialBootstrap.AcquireAsync(new KernelAzureBearerCredentialOptions(provider), callerCancellation.Token);
            try
            {
                var providerCancellation = await WithinAsync(started.Task);
                await CancelAsync(callerCancellation);
                var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => WithinAsync(acquisition));
                Assert.Equal(callerCancellation.Token, error.CancellationToken);
                await WithinAsync(canceled.Task);
                using var lateRegistration = providerCancellation.Register(() => { });
                Assert.NotNull(providerCancellation.WaitHandle);
                Assert.True(providerCancellation.IsCancellationRequested);
                Assert.False(finish.Task.IsCompleted);
            }
            finally
            {
                finish.TrySetException(new Exception("late-synthetic-secret"));
            }
        }

        [Fact]
        public async Task GivenIgnoredCancellation_WhenDeadlineExpires_ReturnsTimeoutAndSignalsProvider()
        {
            var started = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var canceled = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var finish = new TaskCompletionSource<AzureBearerToken>();
            var provider = new DelegateAzureBearerTokenProvider((context, cancellation) =>
            {
                cancellation.Register(() => canceled.TrySetResult(true));
                started.SetResult(true);
                return finish.Task;
            });
            var acquisition = KernelCredentialBootstrap.AcquireAsync(
                new KernelAzureBearerCredentialOptions(provider, acquisitionTimeout: TimeSpan.FromSeconds(1)), CancellationToken.None);
            try
            {
                await WithinAsync(started.Task);
                var error = await Assert.ThrowsAsync<TimeoutException>(() => WithinAsync(acquisition));
                Assert.Null(error.InnerException);
                await WithinAsync(canceled.Task);
                Assert.False(finish.Task.IsCompleted);
            }
            finally
            {
                finish.TrySetResult(new AzureBearerToken("late-token", DateTimeOffset.MaxValue));
            }
        }

        [Fact]
        public async Task GivenThrowingCancellationCallback_WhenCallerCancels_ContainsCallbackFailure()
        {
            using var callerCancellation = new CancellationTokenSource();
            var started = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var canceled = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var finish = new TaskCompletionSource<AzureBearerToken>();
            var provider = new DelegateAzureBearerTokenProvider((context, cancellation) =>
            {
                cancellation.Register(() =>
                {
                    canceled.TrySetResult(true);
                    throw new InvalidOperationException("callback-secret");
                });
                started.SetResult(true);
                return finish.Task;
            });
            var acquisition = KernelCredentialBootstrap.AcquireAsync(new KernelAzureBearerCredentialOptions(provider), callerCancellation.Token);
            try
            {
                await WithinAsync(started.Task);
                await CancelAsync(callerCancellation);
                var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => WithinAsync(acquisition));
                await WithinAsync(canceled.Task);
                Assert.DoesNotContain("callback-secret", error.ToString());
                Assert.Null(error.InnerException);
            }
            finally
            {
                finish.TrySetResult(new AzureBearerToken("late-token", DateTimeOffset.MaxValue));
            }
        }

        [Fact]
        public async Task GivenProviderCompletesInsideCancellation_WhenCanceling_DoesNotRetireSourceInsideCallback()
        {
            using var callerCancellation = new CancellationTokenSource();
            var started = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var sourceAvailable = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var pending = new TaskCompletionSource<AzureBearerToken>();
            var provider = new DelegateAzureBearerTokenProvider((context, cancellation) =>
            {
                cancellation.Register(() =>
                {
                    pending.TrySetCanceled();
                    try
                    {
                        sourceAvailable.TrySetResult(cancellation.WaitHandle != null);
                    }
                    catch (ObjectDisposedException)
                    {
                        sourceAvailable.TrySetResult(false);
                    }
                });
                started.SetResult(true);
                return pending.Task;
            });
            var acquisition = KernelCredentialBootstrap.AcquireAsync(new KernelAzureBearerCredentialOptions(provider), callerCancellation.Token);
            try
            {
                await WithinAsync(started.Task);
                await CancelAsync(callerCancellation);
                var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => WithinAsync(acquisition));
                Assert.Equal(callerCancellation.Token, error.CancellationToken);
                Assert.True(await WithinAsync(sourceAvailable.Task));
            }
            finally
            {
                pending.TrySetResult(new AzureBearerToken("token", DateTimeOffset.MaxValue));
            }
        }

        [Fact]
        public async Task GivenExecutingProviders_WhenCapacityIsFull_RejectsWithInternalTransientFailureWithoutInvokingNext()
        {
            var started = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var finish = new TaskCompletionSource<AzureBearerToken>(TaskCreationOptions.RunContinuationsAsynchronously);
            var calls = 0;
            var provider = new DelegateAzureBearerTokenProvider((context, cancellation) =>
            {
                if (Interlocked.Increment(ref calls) == 64)
                {
                    started.TrySetResult(true);
                }

                return finish.Task;
            });
            var options = new KernelAzureBearerCredentialOptions(provider);
            var active = new List<Task<AzureBearerToken>>();
            try
            {
                for (var index = 0; index < 64; index++)
                {
                    active.Add(KernelCredentialBootstrap.AcquireAsync(options, CancellationToken.None));
                }

                await WithinAsync(started.Task);
                Assert.Equal(64, Volatile.Read(ref calls));
                Assert.All(active, acquisition => Assert.False(acquisition.IsCompleted));
                var excessCalls = 0;
                var excessProvider = new DelegateAzureBearerTokenProvider((context, cancellation) =>
                {
                    Interlocked.Increment(ref excessCalls);
                    return Task.FromResult(new AzureBearerToken("token", DateTimeOffset.MaxValue));
                });
                var error = await Assert.ThrowsAsync<KernelCredentialCapacityException>(() =>
                    KernelCredentialBootstrap.AcquireAsync(new KernelAzureBearerCredentialOptions(excessProvider), CancellationToken.None));

                Assert.IsAssignableFrom<InvalidOperationException>(error);
                Assert.False(error.GetType().IsVisible);
                Assert.Equal("Credential provider execution capacity is exhausted.", error.Message);
                Assert.Null(error.InnerException);
                Assert.Equal(2u, KernelCredentialRequest.GetFailureStatus(error));
                Assert.Equal(0, Volatile.Read(ref excessCalls));
            }
            finally
            {
                finish.TrySetResult(new AzureBearerToken("token", DateTimeOffset.MaxValue));
                await WithinAsync(Task.WhenAll(active));
            }

            started = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            finish = new TaskCompletionSource<AzureBearerToken>(TaskCreationOptions.RunContinuationsAsynchronously);
            calls = 0;
            active.Clear();
            try
            {
                for (var index = 0; index < 64; index++)
                {
                    active.Add(KernelCredentialBootstrap.AcquireAsync(options, CancellationToken.None));
                }

                await WithinAsync(started.Task);
                Assert.Equal(64, Volatile.Read(ref calls));
                Assert.All(active, acquisition => Assert.False(acquisition.IsCompleted));
            }
            finally
            {
                finish.TrySetResult(new AzureBearerToken("token", DateTimeOffset.MaxValue));
                await WithinAsync(Task.WhenAll(active));
            }
        }

        [Fact]
        public async Task GivenCanceledButExecutingProvider_WhenCapacityIsFull_RejectsNextWithoutInvokingIt()
        {
            using var callerCancellation = new CancellationTokenSource();
            var started = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var remaining = 63;
            var finish = new TaskCompletionSource<AzureBearerToken>();
            var provider = new DelegateAzureBearerTokenProvider((context, cancellation) =>
            {
                if (Interlocked.Decrement(ref remaining) == 0)
                {
                    started.SetResult(true);
                }

                return finish.Task;
            });
            var options = new KernelAzureBearerCredentialOptions(provider);
            var active = Enumerable.Range(0, 63).Select(index => KernelCredentialBootstrap.AcquireAsync(options, CancellationToken.None)).ToArray();
            var ignoredStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var canceled = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var ignoredFinish = new TaskCompletionSource<AzureBearerToken>();
            var ignoredProvider = new DelegateAzureBearerTokenProvider((context, cancellation) =>
            {
                cancellation.Register(() => canceled.TrySetResult(true));
                ignoredStarted.SetResult(true);
                return ignoredFinish.Task;
            });
            var ignored = KernelCredentialBootstrap.AcquireAsync(new KernelAzureBearerCredentialOptions(ignoredProvider), callerCancellation.Token);
            try
            {
                await WithinAsync(started.Task);
                await WithinAsync(ignoredStarted.Task);
                await CancelAsync(callerCancellation);
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => WithinAsync(ignored));
                await WithinAsync(canceled.Task);
                var invoked = false;
                var excessProvider = new DelegateAzureBearerTokenProvider((context, cancellation) =>
                {
                    invoked = true;
                    return Task.FromResult(new AzureBearerToken("token", DateTimeOffset.MaxValue));
                });
                var error = await Assert.ThrowsAsync<KernelCredentialCapacityException>(() => KernelCredentialBootstrap.AcquireAsync(new KernelAzureBearerCredentialOptions(excessProvider), CancellationToken.None));
                Assert.Contains("capacity", error.Message);
                Assert.False(invoked);
            }
            finally
            {
                ignoredFinish.TrySetException(new Exception("late-synthetic-secret"));
                finish.TrySetResult(new AzureBearerToken("token", DateTimeOffset.MaxValue));
                await WithinAsync(Task.WhenAll(active));
            }

            var recovered = await KernelCredentialBootstrap.AcquireAsync(options, CancellationToken.None);
            Assert.Equal("token", recovered.Token);
        }

        [Fact]
        public async Task GivenProviderMutatesCallerOptions_WhenAcquiring_UsesPreviouslyCapturedSnapshot()
        {
            var options = new TableOptions
            {
                TableLocation = "az://container/table",
                Version = 7,
                StorageOptions = new Dictionary<string, string> { ["account_name"] = "original" },
            };
            var snapshot = KernelCredentialBootstrap.Snapshot(options);
            KernelCredentialBootstrap.ValidateStorage(snapshot, new Dictionary<string, string>());
            var provider = new DelegateAzureBearerTokenProvider((context, cancellation) =>
            {
                options.StorageOptions.Clear();
                options.Version = 99;
                return Task.FromResult(new AzureBearerToken("token", DateTimeOffset.MaxValue));
            });
            var token = await KernelCredentialBootstrap.AcquireAsync(new KernelAzureBearerCredentialOptions(provider), CancellationToken.None);
            var privateOptions = KernelCredentialBootstrap.WithToken(snapshot, token);
            Assert.Equal((ulong)7, privateOptions.Version);
            Assert.Equal("original", privateOptions.StorageOptions["account_name"]);
            Assert.False(snapshot.StorageOptions.ContainsKey("bearer_token"));
            Assert.False(options.StorageOptions.ContainsKey("bearer_token"));
        }

        [Fact]
        public void GivenSuppressedAmbientCredentials_WhenValidatingStorage_IgnoresIneffectiveConflictingAliases()
        {
            var options = new TableOptions
            {
                TableLocation = "az://container/table",
                StorageOptions = new Dictionary<string, string> { ["account_name"] = "account", ["skip_signature"] = "false" },
            };
            KernelCredentialBootstrap.ValidateStorage(options, new Dictionary<string, string>
            {
                ["AZURE_STORAGE_TOKEN"] = "secret-one",
                ["AZURE_STORAGE_SAS_KEY"] = "secret-one",
                ["AZURE_STORAGE_SAS_TOKEN"] = "secret-two",
                ["AZURE_STORAGE_ACCOUNT_KEY"] = "secret-one",
                ["AZURE_STORAGE_ACCESS_KEY"] = "secret-two",
                ["AZURE_TENANT_ID"] = "tenant",
                ["AZURE_AUTHORITY_HOST"] = "https://login.example",
                ["AZURE_SKIP_SIGNATURE"] = "true",
            });
        }

        [Theory]
        [InlineData("false")]
        [InlineData("FALSE")]
        public void GivenDisabledAmbientBypass_WhenValidatingStorage_AcceptsDisabledMode(string value)
        {
            var options = new TableOptions
            {
                TableLocation = "az://container/table",
                StorageOptions = new Dictionary<string, string> { ["account_name"] = "account" },
            };
            KernelCredentialBootstrap.ValidateStorage(options, new Dictionary<string, string>
            {
                ["AZURE_STORAGE_USE_EMULATOR"] = value,
                ["AZURE_SKIP_SIGNATURE"] = value,
                ["AZURE_USE_AZURE_CLI"] = value,
            });
        }

        [Theory]
        [InlineData("http://dev.example:12345")]
        [InlineData("http://localhost:12345")]
        public void GivenExplicitDevelopmentHttp_WhenValidatingStorage_PreservesApprovedEndpoint(string endpoint)
        {
            var options = new TableOptions
            {
                TableLocation = "az://container/table",
                StorageOptions = new Dictionary<string, string> { ["account_name"] = "account", ["endpoint"] = endpoint, ["allow_http"] = "true" },
            };
            KernelCredentialBootstrap.ValidateStorage(options, new Dictionary<string, string>());
            Assert.Equal(endpoint, options.StorageOptions["endpoint"]);
        }

        [Theory]
        [InlineData("azure_storage_account_key")]
        [InlineData("master_key")]
        [InlineData("access_key")]
        [InlineData("azure_storage_client_id")]
        [InlineData("azure_client_id")]
        [InlineData("azure_storage_client_secret")]
        [InlineData("client_secret")]
        [InlineData("azure_storage_tenant_id")]
        [InlineData("azure_tenant_id")]
        [InlineData("azure_authority_id")]
        [InlineData("tenant_id")]
        [InlineData("authority_id")]
        [InlineData("azure_storage_authority_host")]
        [InlineData("authority_host")]
        [InlineData("azure_storage_sas_key")]
        [InlineData("sas_token")]
        [InlineData("azure_msi_endpoint")]
        [InlineData("identity_endpoint")]
        [InlineData("msi_endpoint")]
        [InlineData("azure_object_id")]
        [InlineData("msi_resource_id")]
        [InlineData("azure_federated_token_file")]
        [InlineData("azure_storage_use_emulator")]
        [InlineData("skip_signature")]
        [InlineData("azure_use_azure_cli")]
        [InlineData("azure_fabric_token_service_url")]
        [InlineData("fabric_session_token")]
        public void GivenRemainingTypedAuthAliases_WhenValidatingStorage_RejectsBothKeyCases(string key)
        {
            foreach (var alias in new[] { key, key.ToUpperInvariant() })
            {
                var options = new TableOptions
                {
                    TableLocation = "az://container/table",
                    StorageOptions = new Dictionary<string, string> { ["account_name"] = "account", [alias] = "secret-value" },
                };
                Assert.Throws<InvalidOperationException>(() => KernelCredentialBootstrap.ValidateStorage(options, new Dictionary<string, string>()));
            }
        }

        [Fact]
        public void GivenThrowingScopeEnumerable_WhenConstructing_SanitizesCallerException()
        {
            var provider = new TrackingProvider();
            var error = Assert.Throws<ArgumentException>(() => new KernelAzureBearerCredentialOptions(provider, ThrowingScopes()));
            Assert.Equal("scopes", error.ParamName);
            Assert.DoesNotContain("enumerator-secret", error.ToString());
            Assert.Null(error.InnerException);
        }

        [Fact]
        public async Task GivenDisposableProvider_WhenAcquiringAndFormatting_DoesNotDisposeOrFormatProvider()
        {
            var provider = new TrackingProvider();
            var options = new KernelAzureBearerCredentialOptions(provider);
            Assert.Contains("[REDACTED]", options.ToString());
            await KernelCredentialBootstrap.AcquireAsync(options, CancellationToken.None);
            Assert.False(provider.Disposed);
        }

        [Theory]
        [InlineData("use_emulator", "true")]
        [InlineData("skip_signature", "not-a-boolean")]
        [InlineData("account_key", "secret-value")]
        public async Task GivenBlockedConfiguration_WhenPreflightingBeforeAcquisition_DoesNotInvokeProvider(string key, string value)
        {
            var invoked = false;
            var provider = new DelegateAzureBearerTokenProvider((context, cancellation) =>
            {
                invoked = true;
                return Task.FromResult(new AzureBearerToken("token", DateTimeOffset.MaxValue));
            });
            var credentials = new KernelAzureBearerCredentialOptions(provider);
            var options = new TableOptions
            {
                TableLocation = "az://container/table",
                StorageOptions = new Dictionary<string, string> { ["account_name"] = "account", [key] = value },
            };
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                var snapshot = KernelCredentialBootstrap.Snapshot(options);
                KernelCredentialBootstrap.ValidateStorage(snapshot, new Dictionary<string, string>());
                await KernelCredentialBootstrap.AcquireAsync(credentials, CancellationToken.None);
            });
            Assert.False(invoked);
        }

        [Fact]
        public async Task GivenBlockedAmbientMode_WhenPreflightingBeforeAcquisition_DoesNotInvokeProvider()
        {
            var invoked = false;
            var provider = new DelegateAzureBearerTokenProvider((context, cancellation) =>
            {
                invoked = true;
                return Task.FromResult(new AzureBearerToken("token", DateTimeOffset.MaxValue));
            });
            var options = new TableOptions
            {
                TableLocation = "az://container/table",
                StorageOptions = new Dictionary<string, string> { ["account_name"] = "account" },
            };
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                KernelCredentialBootstrap.ValidateStorage(KernelCredentialBootstrap.Snapshot(options),
                    new Dictionary<string, string> { ["AZURE_STORAGE_USE_EMULATOR"] = "true" });
                await KernelCredentialBootstrap.AcquireAsync(new KernelAzureBearerCredentialOptions(provider), CancellationToken.None);
            });
            Assert.False(invoked);
        }

        [Fact]
        public void GivenNullOptionalCreateCollections_WhenSnapshotting_PreservesAbsence()
        {
            var options = new TableCreateOptions("az://container/table", new Schema(System.Array.Empty<Field>(), null));
            var snapshot = KernelCredentialBootstrap.Snapshot(options);
            Assert.Null(snapshot.Configuration);
            Assert.Null(snapshot.CustomMetadata);
            Assert.Empty(snapshot.PartitionBy);
        }

        private static IEnumerable<string> ThrowingScopes()
        {
            yield return "https://storage.azure.com/.default";
            throw new InvalidOperationException("enumerator-secret");
        }

        private sealed class TrackingProvider : IAzureBearerTokenProvider, IDisposable
        {
            internal bool Disposed { get; private set; }

            public Task<AzureBearerToken> GetTokenAsync(AzureTokenRequestContext context, CancellationToken cancellationToken)
                => Task.FromResult(new AzureBearerToken("token", DateTimeOffset.MaxValue));

            public void Dispose() => Disposed = true;

            public override string ToString() => throw new InvalidOperationException("provider-secret");
        }

        private static async Task<T> WithinAsync<T>(Task<T> pending)
        {
            using var stopWatchdog = new CancellationTokenSource();
            var watchdog = Task.Delay(TimeSpan.FromSeconds(10), stopWatchdog.Token);
            try
            {
                if (await Task.WhenAny(pending, watchdog) != pending)
                {
                    throw new Xunit.Sdk.XunitException("A credential test barrier did not complete.");
                }

                return await pending;
            }
            finally
            {
                await CancelAsync(stopWatchdog);
            }
        }

        private static Task CancelAsync(CancellationTokenSource cancellation)
        {
#if NET8_0_OR_GREATER
            return cancellation.CancelAsync();
#else
            cancellation.Cancel();
            return Task.CompletedTask;
#endif
        }
    }

    [CollectionDefinition("KernelCredentialContract", DisableParallelization = true)]
    public class KernelCredentialContractCollection
    {
    }
}