using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Xml.Linq;

namespace DeltaLake.Tests.Table;

internal sealed class KernelAzureTestServer
{
    private readonly Dictionary<string, Dictionary<string, byte[]>> blocks = new(StringComparer.Ordinal);
    private readonly ConcurrentBag<TcpClient> clients = new();
    private readonly ConcurrentBag<Task> connections = new();
    private readonly TcpListener listener = new(IPAddress.Loopback, 0);
    private readonly Task listening;
    private readonly ConcurrentQueue<Request> requests = new();
    private readonly object storageGate = new();
    private readonly CancellationTokenSource stopping = new();

    internal KernelAzureTestServer()
    {
        Root = DirectoryHelpers.CreateTempSubdirectory();
        listener.Start();
        Endpoint = new Uri($"http://127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}/");
        listening = Task.Run(AcceptAsync);
    }

    internal Uri Endpoint { get; }

    internal DirectoryInfo Root { get; }

    internal Request[] Requests => requests.ToArray();

    internal async Task DisposeAsync()
    {
#pragma warning disable VSTHRD103
        stopping.Cancel();
#pragma warning restore VSTHRD103
        listener.Stop();
        foreach (var client in clients)
        {
            client.Close();
        }

        await listening;
        await Task.WhenAll(connections.ToArray());
        stopping.Dispose();
        Root.Delete(true);
    }

    private async Task AcceptAsync()
    {
        try
        {
            while (!stopping.IsCancellationRequested)
            {
                var client = await listener.AcceptTcpClientAsync();
                clients.Add(client);
                connections.Add(Task.Run(() => ServeAsync(client)));
            }
        }
        catch (SocketException) when (stopping.IsCancellationRequested)
        {
        }
        catch (ObjectDisposedException) when (stopping.IsCancellationRequested)
        {
        }
    }

    private async Task ServeAsync(TcpClient client)
    {
        using (client)
        {
            try
            {
                using var stream = client.GetStream();
                var firstLine = await ReadLineAsync(stream);
                if (firstLine.Length == 0)
                {
                    return;
                }

                var parts = firstLine.Split(' ');
                var uri = new Uri(Endpoint, parts[1]);
                var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                for (var line = await ReadLineAsync(stream); line.Length != 0; line = await ReadLineAsync(stream))
                {
                    var separator = line.IndexOf(':');
                    headers.Add(line.Substring(0, separator), line.Substring(separator + 1).Trim());
                }

                var authorization = headers.TryGetValue("Authorization", out var header) ? header : string.Empty;
                var label = authorization == "Bearer A.synthetic==" || authorization == "Bearer B.synthetic=="
                    || authorization == "Bearer C.synthetic==" ? authorization : "[REDACTED]";
                var path = Uri.UnescapeDataString(uri.AbsolutePath);
                var query = ParseQuery(uri.Query);
                requests.Enqueue(new Request(parts[0], path, label, query.TryGetValue("comp", out var component) ? component : string.Empty));
                if (label == "[REDACTED]")
                {
                    await RespondAsync(stream, 403, Array.Empty<byte>(), false);
                    return;
                }

                if (parts[0] == "GET" && component == "list")
                {
                    await RespondAsync(stream, 200, List(query), false, "application/xml");
                    return;
                }

                if (!path.StartsWith("/fixture/table/", StringComparison.Ordinal))
                {
                    await RespondAsync(stream, 404, Array.Empty<byte>(), false);
                    return;
                }

                var relative = path.Substring("/fixture/table/".Length);
                var file = Path.GetFullPath(Path.Combine(Root.FullName, relative.Replace('/', Path.DirectorySeparatorChar)));
                if (!file.StartsWith(Root.FullName + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase))
                {
                    await RespondAsync(stream, 400, Array.Empty<byte>(), false);
                    return;
                }

                if (parts[0] == "PUT")
                {
                    if (headers.TryGetValue("Expect", out var expectation) && expectation == "100-continue")
                    {
                        var interim = Encoding.ASCII.GetBytes("HTTP/1.1 100 Continue\r\n\r\n");
                        await stream.WriteAsync(interim, 0, interim.Length);
                    }

                    var body = await ReadBodyAsync(stream, headers);
                    var uploadStatus = Put(file, body, headers, query);
                    await RespondAsync(stream, uploadStatus, Array.Empty<byte>(), false,
                        extraHeaders: new Dictionary<string, string>
                        {
                            ["x-ms-copy-status"] = "success",
                            ["x-ms-copy-id"] = "synthetic",
                        });
                    return;
                }

                if (parts[0] == "DELETE")
                {
                    var existed = File.Exists(file);
                    File.Delete(file);
                    await RespondAsync(stream, existed ? 202 : 404, Array.Empty<byte>(), false);
                    return;
                }

                if ((parts[0] != "GET" && parts[0] != "HEAD") || !File.Exists(file))
                {
                    await RespondAsync(stream, 404, Array.Empty<byte>(), false);
                    return;
                }

                var content = File.ReadAllBytes(file);
                var responseHeaders = new Dictionary<string, string>();
                var status = 200;
                if (headers.TryGetValue("Range", out var range) || headers.TryGetValue("x-ms-range", out range))
                {
                    var bounds = range.Substring("bytes=".Length).Split('-');
                    var start = bounds[0].Length == 0 ? Math.Max(0, content.LongLength - long.Parse(bounds[1], CultureInfo.InvariantCulture)) : long.Parse(bounds[0], CultureInfo.InvariantCulture);
                    var end = bounds[0].Length == 0 || bounds[1].Length == 0 ? content.LongLength - 1 : Math.Min(long.Parse(bounds[1], CultureInfo.InvariantCulture), content.LongLength - 1);
                    responseHeaders["Content-Range"] = $"bytes {start}-{end}/{content.LongLength}";
                    content = content.Skip(checked((int)start)).Take(checked((int)(end - start + 1))).ToArray();
                    status = 206;
                }

                await RespondAsync(stream, status, content, parts[0] == "HEAD", extraHeaders: responseHeaders);
            }
            catch (IOException error) when (stopping.IsCancellationRequested
                || error.InnerException is SocketException socketError
                && (socketError.SocketErrorCode == SocketError.ConnectionAborted
                    || socketError.SocketErrorCode == SocketError.ConnectionReset))
            {
            }
            catch (ObjectDisposedException) when (stopping.IsCancellationRequested)
            {
            }
        }
    }

    private int Put(string file, byte[] body, Dictionary<string, string> headers, Dictionary<string, string> query)
    {
        lock (storageGate)
        {
            if (headers.TryGetValue("If-None-Match", out var condition) && condition == "*" && File.Exists(file))
            {
                return 412;
            }

            if (query.TryGetValue("comp", out var component) && component == "block")
            {
                if (!blocks.TryGetValue(file, out var pending))
                {
                    pending = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                    blocks.Add(file, pending);
                }

                pending[query["blockid"]] = body;
                return 201;
            }

            Directory.CreateDirectory(Path.GetDirectoryName(file)!);
            if (component == "blocklist")
            {
                using var xml = new MemoryStream(body);
                var blockList = XDocument.Load(xml);
                using var output = new MemoryStream();
                foreach (var block in blockList.Root!.Elements())
                {
                    var bytes = blocks[file][block.Value];
                    output.Write(bytes, 0, bytes.Length);
                }

                body = output.ToArray();
                blocks.Remove(file);
            }

            if (headers.TryGetValue("x-ms-copy-source", out var source))
            {
                var sourcePath = Uri.UnescapeDataString(new Uri(source).AbsolutePath);
                if (!sourcePath.StartsWith("/fixture/table/", StringComparison.Ordinal))
                {
                    return 400;
                }

                var sourceFile = Path.GetFullPath(Path.Combine(Root.FullName, sourcePath.Substring("/fixture/table/".Length).Replace('/', Path.DirectorySeparatorChar)));
                if (!sourceFile.StartsWith(Root.FullName + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase))
                {
                    return 400;
                }

                if (!File.Exists(sourceFile))
                {
                    return 404;
                }

                body = File.ReadAllBytes(sourceFile);
            }

            File.WriteAllBytes(file, body);
            return 201;
        }
    }

    private byte[] List(Dictionary<string, string> query)
    {
        var prefix = query.TryGetValue("prefix", out var value) ? value : string.Empty;
        var blobs = Directory.GetFiles(Root.FullName, "*", SearchOption.AllDirectories)
            .Select(file => new { File = new FileInfo(file), Name = "table/" + RelativePath(Root.FullName, file) })
            .Where(blob => blob.Name.StartsWith(prefix, StringComparison.Ordinal))
            .OrderBy(blob => blob.Name, StringComparer.Ordinal)
            .Select(blob => new XElement("Blob",
                new XElement("Name", blob.Name),
                new XElement("Properties",
                    new XElement("Last-Modified", blob.File.LastWriteTimeUtc.ToString("R", CultureInfo.InvariantCulture)),
                    new XElement("Etag", "\"synthetic\""),
                    new XElement("Content-Length", blob.File.Length),
                    new XElement("Content-Type", "application/octet-stream"),
                    new XElement("BlobType", "BlockBlob"),
                    new XElement("LeaseStatus", "unlocked"))));
        var document = new XDocument(new XElement("EnumerationResults",
            new XAttribute("ServiceEndpoint", Endpoint.AbsoluteUri),
            new XAttribute("ContainerName", "fixture"),
            new XElement("Prefix", prefix),
            new XElement("Marker"),
            new XElement("MaxResults", 5000),
            new XElement("Blobs", blobs),
            new XElement("NextMarker")));
        return Encoding.UTF8.GetBytes(document.ToString());
    }

    internal static string RelativePath(string directory, string file)
    {
        var root = new Uri(directory.TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar);
        return Uri.UnescapeDataString(root.MakeRelativeUri(new Uri(file)).ToString());
    }

    private static Dictionary<string, string> ParseQuery(string query)
    {
        var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var parameter in query.TrimStart('?').Split(new[] { '&' }, StringSplitOptions.RemoveEmptyEntries))
        {
            var parts = parameter.Split(new[] { '=' }, 2);
            values[Uri.UnescapeDataString(parts[0])] = parts.Length == 2 ? Uri.UnescapeDataString(parts[1]) : string.Empty;
        }

        return values;
    }

    private static async Task<string> ReadLineAsync(Stream stream)
    {
        using var bytes = new MemoryStream();
        var buffer = new byte[1];
        while (await stream.ReadAsync(buffer, 0, 1) != 0)
        {
            if (buffer[0] == '\n')
            {
                return Encoding.ASCII.GetString(bytes.ToArray()).TrimEnd('\r');
            }

            bytes.WriteByte(buffer[0]);
            if (bytes.Length > 65536)
            {
                throw new InvalidDataException("Fixture HTTP line exceeds its limit.");
            }
        }

        return string.Empty;
    }

    private static async Task<byte[]> ReadBodyAsync(Stream stream, Dictionary<string, string> headers)
    {
        using var body = new MemoryStream();
        if (headers.TryGetValue("Transfer-Encoding", out var encoding) && encoding.IndexOf("chunked", StringComparison.OrdinalIgnoreCase) >= 0)
        {
            while (true)
            {
                var line = await ReadLineAsync(stream);
                var length = int.Parse(line.Split(';')[0], NumberStyles.HexNumber, CultureInfo.InvariantCulture);
                if (length == 0)
                {
                    while ((await ReadLineAsync(stream)).Length != 0)
                    {
                    }

                    break;
                }

                await CopyBytesAsync(stream, body, length);
                if ((await ReadLineAsync(stream)).Length != 0)
                {
                    throw new InvalidDataException("Fixture chunk is missing its terminator.");
                }
            }
        }
        else if (headers.TryGetValue("Content-Length", out var count))
        {
            await CopyBytesAsync(stream, body, int.Parse(count, CultureInfo.InvariantCulture));
        }

        return body.ToArray();
    }

    private static async Task CopyBytesAsync(Stream source, Stream destination, int count)
    {
        if (count < 0 || count > 64 * 1024 * 1024 || destination.Length + count > 64 * 1024 * 1024)
        {
            throw new InvalidDataException("Fixture HTTP body exceeds its limit.");
        }

        var buffer = new byte[8192];
        while (count > 0)
        {
            var read = await source.ReadAsync(buffer, 0, Math.Min(buffer.Length, count));
            if (read == 0)
            {
                throw new EndOfStreamException();
            }

            await destination.WriteAsync(buffer, 0, read);
            count -= read;
        }
    }

    private static async Task RespondAsync(Stream stream, int status, byte[] body, bool head, string contentType = "application/octet-stream", Dictionary<string, string>? extraHeaders = null)
    {
        var reason = status switch
        {
            200 => "OK",
            201 => "Created",
            202 => "Accepted",
            206 => "Partial Content",
            400 => "Bad Request",
            403 => "Forbidden",
            412 => "Precondition Failed",
            _ => "Not Found",
        };
        var response = new StringBuilder($"HTTP/1.1 {status} {reason}\r\nContent-Length: {body.Length}\r\nContent-Type: {contentType}\r\nConnection: close\r\n");
        response.Append("ETag: \"synthetic\"\r\nLast-Modified: Fri, 02 Oct 2026 00:00:00 GMT\r\nx-ms-request-id: synthetic\r\nx-ms-version: 2023-11-03\r\nAccept-Ranges: bytes\r\n");
        if (status == 404)
        {
            response.Append("x-ms-error-code: BlobNotFound\r\n");
        }
        else if (status == 412)
        {
            response.Append("x-ms-error-code: ConditionNotMet\r\n");
        }

        if (extraHeaders != null)
        {
            foreach (var header in extraHeaders)
            {
                response.Append(header.Key).Append(": ").Append(header.Value).Append("\r\n");
            }
        }

        response.Append("\r\n");
        var bytes = Encoding.ASCII.GetBytes(response.ToString());
        await stream.WriteAsync(bytes, 0, bytes.Length);
        if (!head)
        {
            await stream.WriteAsync(body, 0, body.Length);
        }
    }

    internal sealed record Request(string Method, string Path, string Authorization, string Component);
}