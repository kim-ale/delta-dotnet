using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using DeltaLake.Credentials;
using DeltaLake.Table;

namespace DeltaLake.Kernel.Credentials
{
    internal sealed class KernelCredentialCapacityException : InvalidOperationException
    {
        internal KernelCredentialCapacityException()
            : base("Credential provider execution capacity is exhausted.")
        {
        }
    }

    internal static class KernelCredentialBootstrap
    {
        private static readonly SemaphoreSlim ProviderCapacity = new SemaphoreSlim(64, 64);
        internal static readonly TimeSpan MinimumTokenLifetime = TimeSpan.FromSeconds(90);

        [SuppressMessage("Maintainability", "CA1510", Justification = "Explicit null guards preserve net472 compatibility; ArgumentNullException.ThrowIfNull is unavailable on that target.")]
        internal static TableOptions Snapshot(TableOptions options)
        {
            if (options == null)
            {
                throw new ArgumentNullException(nameof(options));
            }

            return new TableOptions
            {
                TableLocation = options.TableLocation,
                StorageOptions = Copy(options.StorageOptions),
                Version = options.Version,
                WithoutFiles = options.WithoutFiles,
                LogBufferSize = options.LogBufferSize,
            };
        }

        [SuppressMessage("Maintainability", "CA1510", Justification = "Explicit null guards preserve net472 compatibility; ArgumentNullException.ThrowIfNull is unavailable on that target.")]
        internal static TableCreateOptions Snapshot(TableCreateOptions options)
        {
            if (options == null)
            {
                throw new ArgumentNullException(nameof(options));
            }

            var schema = new Schema(new List<Field>(options.Schema.FieldsList),
                options.Schema.Metadata?.ToDictionary(entry => entry.Key, entry => entry.Value, StringComparer.Ordinal));
            return new TableCreateOptions(options.TableLocation, schema)
            {
                StorageOptions = Copy(options.StorageOptions),
                PartitionBy = new List<string>(options.PartitionBy).AsReadOnly(),
                SaveMode = options.SaveMode,
                Name = options.Name,
                Description = options.Description,
                Configuration = options.Configuration == null ? null : Copy(options.Configuration),
                CustomMetadata = options.CustomMetadata == null ? null : Copy(options.CustomMetadata),
            };
        }

        internal static TableOptions WithToken(TableOptions snapshot, AzureBearerToken token)
        {
            ValidateToken(token, 65536);
            var privateOptions = Snapshot(snapshot);
            privateOptions.StorageOptions["bearer_token"] = token.Token;
            return privateOptions;
        }

        internal static TableCreateOptions WithToken(TableCreateOptions snapshot, AzureBearerToken token)
        {
            ValidateToken(token, 65536);
            var privateOptions = Snapshot(snapshot);
            privateOptions.StorageOptions["bearer_token"] = token.Token;
            return privateOptions;
        }

        internal static void ValidateStorage(TableStorageOptions snapshot)
        {
            var environment = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (DictionaryEntry entry in Environment.GetEnvironmentVariables())
            {
                if (entry.Key is string key && key.StartsWith("AZURE_", StringComparison.OrdinalIgnoreCase)
                    && entry.Value is string value)
                {
                    environment[key] = value;
                }
            }

            ValidateStorage(snapshot, environment);
        }

        [SuppressMessage("Maintainability", "CA1510", Justification = "Explicit null guards preserve net472 compatibility; ArgumentNullException.ThrowIfNull is unavailable on that target.")]
        [SuppressMessage("Maintainability", "CA2249", Justification = "Character searches use IndexOf for net472 compatibility; string.Contains(char) is unavailable on that target.")]
        internal static void ValidateStorage(TableStorageOptions snapshot, IReadOnlyDictionary<string, string> environment)
        {
            if (snapshot == null)
            {
                throw new ArgumentNullException(nameof(snapshot));
            }

            if (environment == null)
            {
                throw new ArgumentNullException(nameof(environment));
            }

            if (!Uri.TryCreate(snapshot.TableLocation, UriKind.Absolute, out var location)
                || !IsAzureScheme(location.Scheme) || location.Host.Length == 0
                || location.Query.Length != 0 || location.Fragment.Length != 0 || !location.IsDefaultPort)
            {
                throw StorageFailure();
            }

            var explicitOptions = ReadTypedOptions(snapshot.StorageOptions, false);
            foreach (var option in explicitOptions)
            {
                ValidateAuthOption(option.Key, option.Value);
            }

            var ambientOptions = ReadTypedOptions(environment, true, explicitOptions);
            foreach (var option in ambientOptions)
            {
                if (!explicitOptions.ContainsKey(option.Key) && !IsSuppressedByBootstrapToken(option.Key))
                {
                    ValidateAuthOption(option.Key, option.Value);
                }
            }

            if (location.UserInfo.Length != 0)
            {
                if ((location.Scheme != "az" && location.Scheme != "abfs" && location.Scheme != "abfss")
                    || location.UserInfo.IndexOf(':') >= 0 || !HasEmbeddedAccount(location.Host))
                {
                    throw StorageFailure();
                }
            }
            else if (location.Host.IndexOf('.') >= 0
                || !explicitOptions.TryGetValue(AzureOption.AccountName, out var account) || string.IsNullOrWhiteSpace(account))
            {
                throw new InvalidOperationException("Kernel bearer credentials require an explicit storage account.");
            }

            var hasExplicitHttp = explicitOptions.TryGetValue(AzureOption.AllowHttp, out var allowHttp)
                && string.Equals(allowHttp, "true", StringComparison.OrdinalIgnoreCase);
            if (explicitOptions.TryGetValue(AzureOption.Endpoint, out var endpoint)
                || ambientOptions.TryGetValue(AzureOption.Endpoint, out endpoint))
            {
                if (!Uri.TryCreate(endpoint, UriKind.Absolute, out var endpointUri)
                    || endpointUri.Host.Length == 0 || endpointUri.UserInfo.Length != 0
                    || endpointUri.Query.Length != 0 || endpointUri.Fragment.Length != 0
                    || (endpointUri.Scheme != Uri.UriSchemeHttps && (endpointUri.Scheme != Uri.UriSchemeHttp || !hasExplicitHttp)))
                {
                    throw new InvalidOperationException("Kernel bearer credentials require HTTPS or explicitly approved HTTP storage.");
                }
            }
        }

        [SuppressMessage("Maintainability", "CA1510", Justification = "Explicit null guards preserve net472 compatibility; ArgumentNullException.ThrowIfNull is unavailable on that target.")]
        [SuppressMessage("Design", "CA1031", Justification = "User credential providers may throw arbitrary exceptions; all acquisition failures must be sanitized while preserving provider execution cleanup.")]
        internal static async Task<AzureBearerToken> AcquireAsync(KernelAzureBearerCredentialOptions options, CancellationToken cancellationToken)
        {
            if (options == null)
            {
                throw new ArgumentNullException(nameof(options));
            }

            cancellationToken.ThrowIfCancellationRequested();
            var context = new AzureTokenRequestContext(options.Scopes, MinimumTokenLifetime);
            if (!await ProviderCapacity.WaitAsync(0, CancellationToken.None).ConfigureAwait(false))
            {
                throw new KernelCredentialCapacityException();
            }

            ProviderExecution execution;
            try
            {
                execution = new ProviderExecution();
            }
            catch (Exception)
            {
                ProviderCapacity.Release();
                throw AcquisitionFailure();
            }

            using var stopDeadline = new CancellationTokenSource();
            var elapsed = Stopwatch.StartNew();
            var deadline = Task.Delay(options.AcquisitionTimeout, stopDeadline.Token);
            var callerCanceled = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            using var cancellationRegistration = cancellationToken.Register(() => callerCanceled.TrySetResult(true));
            Task<AzureBearerToken> actual;
            try
            {
                actual = execution.StartAsync(options.Provider, context);
            }
            catch (Exception)
            {
                execution.Complete();
#if NET8_0_OR_GREATER
                await stopDeadline.CancelAsync().ConfigureAwait(false);
#else
                stopDeadline.Cancel();
#endif
                throw AcquisitionFailure();
            }

            try
            {
                var completed = await Task.WhenAny(actual, deadline, callerCanceled.Task).ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();
                if (completed != actual || elapsed.Elapsed >= options.AcquisitionTimeout)
                {
                    throw new TimeoutException("Credential acquisition exceeded its deadline.");
                }

                AzureBearerToken token;
                try
                {
                    token = await actual.ConfigureAwait(false);
                    ValidateToken(token, options.MaxTokenBytes);
                }
                catch (Exception)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (elapsed.Elapsed >= options.AcquisitionTimeout)
                    {
                        throw new TimeoutException("Credential acquisition exceeded its deadline.");
                    }

                    throw AcquisitionFailure();
                }

                cancellationToken.ThrowIfCancellationRequested();
                if (elapsed.Elapsed >= options.AcquisitionTimeout)
                {
                    throw new TimeoutException("Credential acquisition exceeded its deadline.");
                }

                return token;
            }
            finally
            {
#if NET8_0_OR_GREATER
                await stopDeadline.CancelAsync().ConfigureAwait(false);
#else
                stopDeadline.Cancel();
#endif
                if (!actual.IsCompleted)
                {
                    execution.RequestCancellation();
                }
            }
        }

        internal static void ValidateToken(AzureBearerToken token, int maxTokenBytes)
        {
            if (token == null || maxTokenBytes < 1 || maxTokenBytes > 65536
                || token.Token.Length == 0 || token.Token.Length > maxTokenBytes
                || token.ExpiresOn <= DateTimeOffset.UtcNow + MinimumTokenLifetime)
            {
                throw new InvalidOperationException("The credential provider returned an unusable token.");
            }

            var padding = false;
            var hasContent = false;
            foreach (var character in token.Token)
            {
                if (character == '=')
                {
                    padding = true;
                }
                else if (!padding && ((character >= 'A' && character <= 'Z')
                    || (character >= 'a' && character <= 'z') || (character >= '0' && character <= '9')
                    || character == '-' || character == '.' || character == '_' || character == '~'
                    || character == '+' || character == '/'))
                {
                    hasContent = true;
                }
                else
                {
                    throw new InvalidOperationException("The credential provider returned an unusable token.");
                }
            }

            if (!hasContent)
            {
                throw new InvalidOperationException("The credential provider returned an unusable token.");
            }
        }

        private static Dictionary<string, string> Copy(Dictionary<string, string> options)
            => new Dictionary<string, string>(options, options.Comparer);

        private static InvalidOperationException AcquisitionFailure()
            => new InvalidOperationException("The credential provider failed to acquire a usable token.");

        private static InvalidOperationException StorageFailure()
            => new InvalidOperationException("The storage configuration is incompatible with kernel bearer credentials.");

        private static bool IsAzureScheme(string scheme)
            => scheme == "az" || scheme == "adl" || scheme == "azure" || scheme == "abfs" || scheme == "abfss";

        private static bool HasEmbeddedAccount(string host)
        {
            var separator = host.IndexOf('.');
            if (separator <= 0)
            {
                return false;
            }

            var suffix = host.Substring(separator);
            return suffix == ".dfs.core.windows.net" || suffix == ".blob.core.windows.net"
                || suffix == ".dfs.fabric.microsoft.com" || suffix == ".blob.fabric.microsoft.com";
        }

        private static Dictionary<AzureOption, string> ReadTypedOptions(
            IEnumerable<KeyValuePair<string, string>> options,
            bool environment,
            IReadOnlyDictionary<AzureOption, string>? explicitOptions = null)
        {
            var typed = new Dictionary<AzureOption, string>();
            foreach (var option in options)
            {
                if (environment && !option.Key.StartsWith("AZURE_", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var kind = ParseOption(option.Key);
                if (kind == AzureOption.Other || (environment && (IsSuppressedByBootstrapToken(kind)
                    || (explicitOptions != null && explicitOptions.ContainsKey(kind)))))
                {
                    continue;
                }

                if (typed.TryGetValue(kind, out var previous) && !string.Equals(previous, option.Value, StringComparison.Ordinal))
                {
                    throw StorageFailure();
                }

                typed[kind] = option.Value;
            }

            return typed;
        }

        private static bool IsSuppressedByBootstrapToken(AzureOption kind)
            => kind == AzureOption.AccessKey || kind == AzureOption.AuthorityId || kind == AzureOption.AuthorityHost
                || kind == AzureOption.ClientId || kind == AzureOption.ClientSecret || kind == AzureOption.FederatedTokenFile
                || kind == AzureOption.SasKey || kind == AzureOption.Token || kind == AzureOption.MsiEndpoint
                || kind == AzureOption.ObjectId || kind == AzureOption.MsiResourceId;

        private static void ValidateAuthOption(AzureOption kind, string value)
        {
            if (kind == AzureOption.UseEmulator || kind == AzureOption.SkipSignature || kind == AzureOption.UseAzureCli)
            {
                if (!string.Equals(value, "false", StringComparison.OrdinalIgnoreCase))
                {
                    throw StorageFailure();
                }
            }
            else if (kind == AzureOption.AccessKey || kind == AzureOption.SasKey || kind == AzureOption.Token
                || kind == AzureOption.ClientId || kind == AzureOption.ClientSecret || kind == AzureOption.AuthorityId
                || kind == AzureOption.AuthorityHost || kind == AzureOption.MsiEndpoint || kind == AzureOption.ObjectId
                || kind == AzureOption.MsiResourceId || kind == AzureOption.FederatedTokenFile
                || kind == AzureOption.FabricTokenServiceUrl || kind == AzureOption.FabricSessionToken)
            {
                throw StorageFailure();
            }
        }

        [SuppressMessage("Globalization", "CA1308", Justification = "Lowercase protocol aliases must match the Azure storage option parser; changing normalization to uppercase would alter alias recognition.")]
        private static AzureOption ParseOption(string key)
        {
            return key.ToLowerInvariant() switch
            {
                "azure_storage_account_name" or "account_name" => AzureOption.AccountName,
                "azure_storage_account_key" or "azure_storage_access_key" or "azure_storage_master_key" or "master_key" or "account_key" or "access_key" => AzureOption.AccessKey,
                "azure_storage_client_id" or "azure_client_id" or "client_id" => AzureOption.ClientId,
                "azure_storage_client_secret" or "azure_client_secret" or "client_secret" => AzureOption.ClientSecret,
                "azure_storage_tenant_id" or "azure_storage_authority_id" or "azure_tenant_id" or "azure_authority_id" or "tenant_id" or "authority_id" => AzureOption.AuthorityId,
                "azure_storage_authority_host" or "azure_authority_host" or "authority_host" => AzureOption.AuthorityHost,
                "azure_storage_sas_key" or "azure_storage_sas_token" or "sas_key" or "sas_token" => AzureOption.SasKey,
                "azure_storage_token" or "bearer_token" or "token" => AzureOption.Token,
                "azure_storage_use_emulator" or "object_store_use_emulator" or "use_emulator" => AzureOption.UseEmulator,
                "azure_storage_endpoint" or "azure_endpoint" or "endpoint" => AzureOption.Endpoint,
                "azure_msi_endpoint" or "azure_identity_endpoint" or "identity_endpoint" or "msi_endpoint" => AzureOption.MsiEndpoint,
                "azure_object_id" or "object_id" => AzureOption.ObjectId,
                "azure_msi_resource_id" or "msi_resource_id" => AzureOption.MsiResourceId,
                "azure_federated_token_file" or "federated_token_file" => AzureOption.FederatedTokenFile,
                "azure_use_azure_cli" or "use_azure_cli" => AzureOption.UseAzureCli,
                "azure_skip_signature" or "skip_signature" => AzureOption.SkipSignature,
                "azure_container_name" or "container_name" => AzureOption.ContainerName,
                "azure_use_fabric_endpoint" or "use_fabric_endpoint" => AzureOption.UseFabricEndpoint,
                "azure_fabric_token_service_url" or "fabric_token_service_url" => AzureOption.FabricTokenServiceUrl,
                "azure_fabric_session_token" or "fabric_session_token" => AzureOption.FabricSessionToken,
                "azure_allow_http" or "allow_http" => AzureOption.AllowHttp,
                _ => AzureOption.Other,
            };
        }

        private enum AzureOption
        {
            Other, AccountName, AccessKey, ClientId, ClientSecret, AuthorityId, AuthorityHost,
            SasKey, Token, UseEmulator, Endpoint, MsiEndpoint, ObjectId, MsiResourceId,
            FederatedTokenFile, UseAzureCli, SkipSignature, ContainerName, UseFabricEndpoint,
            FabricTokenServiceUrl, FabricSessionToken, AllowHttp,
        }

        [SuppressMessage("Design", "CA1001", Justification = "Retire disposes both cancellation sources after provider execution and cancellation callbacks finish; exposing IDisposable could release tokens still in use.")]
        private sealed class ProviderExecution
        {
            private readonly object _gate = new object();
            private readonly CancellationTokenSource _cancellation = new CancellationTokenSource();
            private readonly CancellationTokenSource _linkedCancellation;
            private bool _canceling;
            private bool _completed;

            internal ProviderExecution()
            {
                _linkedCancellation = CancellationTokenSource.CreateLinkedTokenSource(_cancellation.Token);
            }

            internal Task<AzureBearerToken> StartAsync(IAzureBearerTokenProvider provider, AzureTokenRequestContext context)
            {
                var actual = Task.Run(async () =>
                {
                    try
                    {
                        var cancellation = _linkedCancellation.Token;
                        cancellation.ThrowIfCancellationRequested();
                        var pending = provider.GetTokenAsync(context, cancellation);
                        if (pending == null)
                        {
                            throw AcquisitionFailure();
                        }

                        return await pending.ConfigureAwait(false);
                    }
                    finally
                    {
                        Complete();
                    }
                });
                _ = actual.ContinueWith(task => { _ = task.Exception; },
                    CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
                return actual;
            }

            [SuppressMessage("Design", "CA1031", Justification = "Cancellation invokes arbitrary user provider callbacks; all callback exceptions must be contained so execution retirement still completes.")]
            internal void RequestCancellation()
            {
                _ = Task.Run(() =>
                {
                    lock (_gate)
                    {
                        if (_completed)
                        {
                            return;
                        }

                        _canceling = true;
                    }

                    try
                    {
                        _cancellation.Cancel();
                    }
                    catch (Exception)
                    {
                    }
                    finally
                    {
                        lock (_gate)
                        {
                            _canceling = false;
                            if (_completed)
                            {
                                Retire();
                            }
                        }
                    }
                });
            }

            internal void Complete()
            {
                lock (_gate)
                {
                    if (_completed)
                    {
                        return;
                    }

                    _completed = true;
                    if (!_canceling)
                    {
                        Retire();
                    }
                }
            }

            private void Retire()
            {
                _linkedCancellation.Dispose();
                _cancellation.Dispose();
                ProviderCapacity.Release();
            }
        }
    }
}