using System.Runtime.InteropServices;
using DeltaKernel.NativePrototype;

namespace NativeObjectStore.Consumer;

internal static class Program
{
    private static async Task<int> Main()
    {
        try
        {
            VerifyLayouts();
            GivenUnknownDescriptorVersion_WhenAdopted_ReleasesUnadoptedContext();
            GivenInvalidTableUrl_WhenEngineBuilderFails_ReleasesAdoptedContext();
            GivenMissingTable_WhenSnapshotBuildFails_PreservesEngineAndFreesErrors();
            await GivenMemoryStore_WhenMutated_SeesNewVersionOnSameEngineAsync();
            GivenIndependentSessions_WhenOneMutates_IsolatesStateAndReleasesOnce();
            await GivenNativeAzure_WhenSnapshotsRepeat_ObservesCredentialRotationAsync();
            var final = NativeStoreSession.GetStatistics();
            Require(final.ErrorAllocations == final.ErrorReleases, "All returned Kernel errors must be freed.");
            Console.WriteLine("PASS all public-package native object-store probes");
            Console.WriteLine("LIMITS Windows x64; snapshot/list/read fixtures; synthetic native Azure credentials; no live cloud or production Table integration.");
            return 0;
        }
        catch (Exception exception)
        {
            await Console.Error.WriteLineAsync($"FAIL {exception.GetType().Name}: {exception.Message}");
            return 1;
        }
    }

    private static void VerifyLayouts()
    {
        var statistics = NativeStoreSession.GetStatistics();
        Require(Marshal.SizeOf<NativeStoreDescriptor>() == 56, "Managed descriptor must be 56 bytes.");
        Require(statistics.DescriptorSize == 56, "Independent native descriptor must be 56 bytes.");
        Require(statistics.StringSliceSize == 16, "String slices must be 16 bytes.");
        Require(statistics.ObjectMetadataSize == 32, "Object metadata must be 32 bytes.");
        Require(statistics.HandleResultSize == 16, "Tagged handle results must be 16 bytes.");
        Require(Marshal.OffsetOf<NativeStoreDescriptor>(nameof(NativeStoreDescriptor.AbiVersion)).ToInt32() == 0, "Version offset mismatch.");
        Require(Marshal.OffsetOf<NativeStoreDescriptor>(nameof(NativeStoreDescriptor.StructSize)).ToInt32() == 4, "Size offset mismatch.");
        Require(Marshal.OffsetOf<NativeStoreDescriptor>(nameof(NativeStoreDescriptor.Context)).ToInt32() == 8, "Context offset mismatch.");
        Require(Marshal.OffsetOf<NativeStoreDescriptor>(nameof(NativeStoreDescriptor.Get)).ToInt32() == 16, "GET offset mismatch.");
        Require(Marshal.OffsetOf<NativeStoreDescriptor>(nameof(NativeStoreDescriptor.List)).ToInt32() == 24, "LIST offset mismatch.");
        Require(Marshal.OffsetOf<NativeStoreDescriptor>(nameof(NativeStoreDescriptor.Put)).ToInt32() == 32, "PUT offset mismatch.");
        Require(Marshal.OffsetOf<NativeStoreDescriptor>(nameof(NativeStoreDescriptor.DeleteObject)).ToInt32() == 40, "DELETE offset mismatch.");
        Require(Marshal.OffsetOf<NativeStoreDescriptor>(nameof(NativeStoreDescriptor.Release)).ToInt32() == 48, "Release offset mismatch.");
        Console.WriteLine("PASS public x64 ABI layouts: descriptor 56, string 16, metadata 32, result 16");
    }

    private static void GivenUnknownDescriptorVersion_WhenAdopted_ReleasesUnadoptedContext()
    {
        var before = NativeStoreSession.GetStatistics();
        var error = Expect<InvalidOperationException>(() =>
        {
            using var unexpected = NativeStoreSession.CreateMemory(descriptorVersion: 99);
        });
        var after = NativeStoreSession.GetStatistics();
        Require(error.Message.Contains("GenericError", StringComparison.Ordinal), "Descriptor rejection must come from the public Kernel error result.");
        Require(after.Releases == before.Releases + 1, "Rejected descriptor context must release once.");
        Require(after.Callbacks == before.Callbacks, "Descriptor rejection must perform no native store I/O.");
        Require(after.ErrorAllocations == before.ErrorAllocations + 1, "Rejection must allocate one Kernel error.");
        Require(after.ErrorReleases == before.ErrorReleases + 1, "Rejection error memory must be freed.");
        Console.WriteLine("PASS invalid ABI adoption: caller release once, no I/O, error freed");
    }

    private static void GivenInvalidTableUrl_WhenEngineBuilderFails_ReleasesAdoptedContext()
    {
        var before = NativeStoreSession.GetStatistics();
        Expect<InvalidOperationException>(() =>
        {
            using var unexpected = NativeStoreSession.CreateMemory("not a url");
        });
        var after = NativeStoreSession.GetStatistics();
        Require(after.Releases == before.Releases + 1, "Engine-builder failure must release the adopted store once.");
        Require(after.ErrorAllocations > before.ErrorAllocations, "Builder failure must return a native error.");
        Require(after.ErrorAllocations - before.ErrorAllocations == after.ErrorReleases - before.ErrorReleases, "Builder error allocation must be freed.");
        Console.WriteLine("PASS builder error cleanup after store adoption");
    }

    private static void GivenMissingTable_WhenSnapshotBuildFails_PreservesEngineAndFreesErrors()
    {
        var before = NativeStoreSession.GetStatistics();
        using var session = NativeStoreSession.CreateMemory("memory:///missing/");
        Expect<InvalidOperationException>(() => session.GetVersion());
        Expect<InvalidOperationException>(() => session.GetVersion());
        var during = NativeStoreSession.GetStatistics();
        Require(during.Releases == before.Releases, "Snapshot failure must not release the live engine's store.");
        Require(during.ErrorAllocations >= before.ErrorAllocations + 2, "Repeated snapshot failures must return errors.");
        Require(during.ErrorAllocations - before.ErrorAllocations == during.ErrorReleases - before.ErrorReleases, "Snapshot errors must be freed.");
        session.Dispose();
        Require(NativeStoreSession.GetStatistics().Releases == before.Releases + 1, "Failed snapshots must leave only one final context release.");
        Console.WriteLine("PASS consuming snapshot-builder failure cleanup and retained engine");
    }

    private static async Task GivenMemoryStore_WhenMutated_SeesNewVersionOnSameEngineAsync()
    {
        var before = NativeStoreSession.GetStatistics();
        using var session = NativeStoreSession.CreateMemory();
        var originalVersion = session.GetVersion();
        Require(originalVersion == 0, "Seeded native memory version must be zero.");
        Require(NativeStoreSession.GetStatistics().Releases == before.Releases, "Caller store release must leave the builder/engine owner alive.");
        session.AppendCommit();
        Require(session.GetVersion() == 1, "The same engine must observe native version-one mutation.");
        Expect<InvalidOperationException>(session.AppendCommit);
        Require(session.GetVersion() == 1, "Repeated append must fail without changing the version.");
        Require(originalVersion == 0, "The earlier snapshot value must remain zero.");
        var versions = await Task.WhenAll(Enumerable.Range(0, 8).Select(iteration => Task.Run(session.GetVersion)));
        Require(versions.All(version => version == 1), "Concurrent managed callers must serialize safely on the same engine.");
        var during = NativeStoreSession.GetStatistics();
        Require(during.Callbacks > before.Callbacks, "Kernel snapshots must invoke native provider I/O callbacks.");
        Require(during.CredentialRequests == before.CredentialRequests, "Memory must not simulate credential acquisitions.");
        session.Dispose();
        session.Dispose();
        Require(NativeStoreSession.GetStatistics().Releases == before.Releases + 1, "Repeated disposal must release exactly once.");
        Expect<ObjectDisposedException>(() => session.GetVersion());
        Expect<ObjectDisposedException>(session.AppendCommit);
        Console.WriteLine($"PASS memory/list/live mutation 0 -> 1 on one engine; {during.Callbacks - before.Callbacks} native callbacks; serialized callers; final release once");
    }

    private static void GivenIndependentSessions_WhenOneMutates_IsolatesStateAndReleasesOnce()
    {
        var before = NativeStoreSession.GetStatistics();
        using var first = NativeStoreSession.CreateMemory();
        using var second = NativeStoreSession.CreateMemory();
        first.AppendCommit();
        Require(first.GetVersion() == 1 && second.GetVersion() == 0, "Separate native contexts must have independent memory state.");
        first.Dispose();
        Require(NativeStoreSession.GetStatistics().Releases == before.Releases + 1, "Only the first context must release.");
        Require(second.GetVersion() == 0, "The other engine must remain usable after first disposal.");
        second.Dispose();
        Require(NativeStoreSession.GetStatistics().Releases == before.Releases + 2, "Both independent contexts must release once.");
        Console.WriteLine("PASS independent native sessions and exact final release counts");
    }

    private static async Task GivenNativeAzure_WhenSnapshotsRepeat_ObservesCredentialRotationAsync()
    {
        await using var fixture = new AzureFixture();
        var before = NativeStoreSession.GetStatistics();
        using var session = NativeStoreSession.CreateAzure(fixture.Endpoint.AbsoluteUri);
        Require(await Task.Run(session.GetVersion) == 0, "Native Azure must load the fixture's version-zero snapshot.");
        Require(await Task.Run(session.GetVersion) == 0, "A second snapshot must use the same retained engine/store.");
        var during = NativeStoreSession.GetStatistics();
        var requests = fixture.Requests;
        Require(requests.Any(request => request.Method == "GET" && request.PathAndQuery.Contains("comp=list", StringComparison.Ordinal)), "The native Azure store must perform Blob listing.");
        Require(requests.Any(request => request.Method == "GET" && request.PathAndQuery.Contains(AzureFixture.CommitName, StringComparison.Ordinal)), "The native Azure store must read commit bytes.");
        Require(requests.All(request => request.Authorization.StartsWith("Bearer native-token-", StringComparison.Ordinal)), "Every native fixture request must carry the provider's synthetic bearer credential.");
        var generations = requests.Select(request => request.Authorization).Distinct(StringComparer.Ordinal).Count();
        Require(generations >= 2, "Actual outgoing Authorization values must rotate on the same engine.");
        Require(during.CredentialRequests >= before.CredentialRequests + 2, "Native credential acquisition count must advance.");
        Require(during.CredentialGeneration >= before.CredentialGeneration + 2, "Native synthetic credential generations must advance.");
        Require(during.Callbacks > before.Callbacks, "Azure snapshots must cross the native callback boundary.");
        Require(during.Releases == before.Releases, "The Azure context must remain owned by the engine.");
        Expect<InvalidOperationException>(session.AppendCommit);
        session.Dispose();
        session.Dispose();
        Require(NativeStoreSession.GetStatistics().Releases == before.Releases + 1, "Azure must release once after engine disposal.");
        Console.WriteLine($"PASS native Azure list/read and fake refresh on one engine: {during.Callbacks - before.Callbacks} callbacks, {during.CredentialRequests - before.CredentialRequests} credential requests, {generations} distinct Authorization generations");
    }

    private static void Require(bool condition, string message)
    {
        if (!condition)
        {
            throw new InvalidOperationException(message);
        }
    }

    private static TException Expect<TException>(Action action) where TException : Exception
    {
        try
        {
            action();
        }
        catch (TException exception)
        {
            return exception;
        }

        throw new InvalidOperationException($"Expected {typeof(TException).Name}.");
    }
}