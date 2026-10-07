using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Xml.Linq;

namespace NativeObjectStore.Consumer;

internal sealed class AzureFixture : IAsyncDisposable
{
    internal const string CommitName = "table/_delta_log/00000000000000000000.json";
    private const string Modified = "Wed, 01 Jan 2020 00:00:00 GMT";
    private const string EntityTag = "\"native-prototype\"";
    private static readonly byte[] Commit = CreateCommit();
    private readonly HttpListener listener = new();
    private readonly ConcurrentQueue<ObservedRequest> requests = new();
    private readonly Task serverTask;

    internal AzureFixture()
    {
        var portProbe = new TcpListener(IPAddress.Loopback, 0);
        portProbe.Start();
        var port = ((IPEndPoint)portProbe.LocalEndpoint).Port;
        portProbe.Stop();
        Endpoint = new Uri($"http://127.0.0.1:{port.ToString(CultureInfo.InvariantCulture)}/");
        listener.Prefixes.Add(Endpoint.AbsoluteUri);
        try
        {
            listener.Start();
        }
        catch
        {
            listener.Close();
            throw;
        }

        serverTask = ServeAsync();
    }

    internal Uri Endpoint { get; }
    internal ObservedRequest[] Requests => requests.ToArray();

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "VSTHRD003", Justification = "Shutdown joins the task owned by this fixture after stopping its listener.")]
    public async ValueTask DisposeAsync()
    {
        listener.Stop();
        try
        {
            await serverTask.ConfigureAwait(false);
        }
        finally
        {
            listener.Close();
        }
    }

    private async Task ServeAsync()
    {
        while (listener.IsListening)
        {
            HttpListenerContext context;
            try
            {
                context = await listener.GetContextAsync();
            }
            catch (HttpListenerException) when (!listener.IsListening)
            {
                return;
            }
            catch (ObjectDisposedException) when (!listener.IsListening)
            {
                return;
            }

            try
            {
                var request = context.Request;
                requests.Enqueue(new ObservedRequest(
                    request.HttpMethod, request.Url?.PathAndQuery ?? string.Empty,
                    request.Headers["Authorization"] ?? string.Empty));
                await RespondAsync(context);
            }
            finally
            {
                context.Response.Close();
            }
        }
    }

    private async Task RespondAsync(HttpListenerContext context)
    {
        var request = context.Request;
        var response = context.Response;
        var path = request.Url?.AbsolutePath.TrimEnd('/') ?? string.Empty;
        byte[] body;
        response.Headers["x-ms-request-id"] = "native-prototype-fixture";
        response.Headers["x-ms-version"] = "2021-12-02";

        if (request.HttpMethod == "GET" && path == "/container" && request.QueryString["comp"] == "list")
        {
            response.StatusCode = 200;
            response.ContentType = "application/xml";
            body = CreateListing(request.QueryString["prefix"] ?? string.Empty);
        }
        else if ((request.HttpMethod == "GET" || request.HttpMethod == "HEAD") && path == "/container/" + CommitName)
        {
            response.StatusCode = 200;
            response.ContentType = "application/json";
            response.Headers["Last-Modified"] = Modified;
            response.Headers["ETag"] = EntityTag;
            response.Headers["x-ms-blob-type"] = "BlockBlob";
            body = Commit;
        }
        else
        {
            response.StatusCode = 404;
            response.ContentType = "application/xml";
            response.Headers["x-ms-error-code"] = "BlobNotFound";
            body = Encoding.UTF8.GetBytes(new XElement("Error",
                new XElement("Code", "BlobNotFound"),
                new XElement("Message", "The fixture blob is absent.")).ToString(SaveOptions.DisableFormatting));
        }

        response.ContentLength64 = body.Length;
        if (request.HttpMethod != "HEAD")
        {
            await response.OutputStream.WriteAsync(body);
        }
    }

    private byte[] CreateListing(string prefix)
    {
        var blobs = new XElement("Blobs");
        if (CommitName.StartsWith(prefix, StringComparison.Ordinal))
        {
            blobs.Add(new XElement("Blob",
                new XElement("Name", CommitName),
                new XElement("Properties",
                    new XElement("Last-Modified", Modified),
                    new XElement("Etag", EntityTag),
                    new XElement("Content-Length", Commit.Length),
                    new XElement("Content-Type", "application/json"),
                    new XElement("BlobType", "BlockBlob"),
                    new XElement("LeaseStatus", "unlocked"),
                    new XElement("LeaseState", "available"),
                    new XElement("ServerEncrypted", "true"))));
        }

        var document = new XDocument(new XElement("EnumerationResults",
            new XAttribute("ServiceEndpoint", Endpoint.AbsoluteUri),
            new XAttribute("ContainerName", "container"),
            new XElement("Prefix", prefix), new XElement("Marker", string.Empty),
            new XElement("MaxResults", 5000), blobs, new XElement("NextMarker", string.Empty)));
        return Encoding.UTF8.GetBytes(document.ToString(SaveOptions.DisableFormatting));
    }

    private static byte[] CreateCommit()
    {
        var schema = JsonSerializer.Serialize(new
        {
            type = "struct",
            fields = new[] { new { name = "id", type = "long", nullable = true, metadata = new Dictionary<string, object>() } },
        });
        var protocol = JsonSerializer.Serialize(new { protocol = new { minReaderVersion = 1, minWriterVersion = 2 } });
        var metadata = JsonSerializer.Serialize(new
        {
            metaData = new
            {
                id = "84d1686e-c18e-4ea1-b93f-5a5241fb6646",
                format = new { provider = "parquet", options = new Dictionary<string, string>() },
                schemaString = schema,
                partitionColumns = Array.Empty<string>(),
                configuration = new Dictionary<string, string>(),
                createdTime = 0,
            },
        });
        return Encoding.UTF8.GetBytes(protocol + "\n" + metadata + "\n");
    }

    internal sealed record ObservedRequest(string Method, string PathAndQuery, string Authorization);
}