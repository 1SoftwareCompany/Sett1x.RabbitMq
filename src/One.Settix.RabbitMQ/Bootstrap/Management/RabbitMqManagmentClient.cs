﻿using System.Net;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using One.Settix.RabbitMQ.Bootstrap.Management.Model;

namespace One.Settix.RabbitMQ.Bootstrap.Management
{
    internal sealed class RabbitMqManagementClient
    {
        private static readonly Regex UrlRegex = new Regex(@"^(http|https):\/\/.+\w$");

        readonly string username;
        readonly string password;
        readonly int portNumber;
        readonly bool useSsl;
        readonly int sslEnabledPort = 443;
        readonly int sslDisabledPort = 15672;
        readonly JsonSerializerOptions settings;

        readonly bool runningOnMono;

        readonly Action<HttpWebRequest> configureRequest;
        readonly TimeSpan defaultTimeout = TimeSpan.FromSeconds(20);
        readonly TimeSpan timeout;

        private readonly List<string> apiAddressCollection;
        private string lastKnownApiAddress;

        internal RabbitMqManagementClient(RabbitMqOptions settings) : this(settings.ApiAddress ?? settings.Server, settings.Username, settings.Password, useSsl: settings.UseSsl) { }

        internal RabbitMqManagementClient(string apiAddresses, string username, string password, bool useSsl = false, TimeSpan? timeout = null, Action<HttpWebRequest> configureRequest = null)
        {
            portNumber = useSsl ? sslEnabledPort : sslDisabledPort;
            this.useSsl = useSsl;
            apiAddressCollection = new List<string>();
            string[] parsedAddresses = apiAddresses.Split(',', StringSplitOptions.RemoveEmptyEntries);
            foreach (var apiAddress in parsedAddresses)
            {
                TryInitializeApiHostName(apiAddress, useSsl);
            }
            if (apiAddressCollection.Any() == false) throw new ArgumentException("Invalid API addresses", nameof(apiAddresses));

            if (string.IsNullOrEmpty(username)) throw new ArgumentException("username is null or empty");
            if (string.IsNullOrEmpty(password)) throw new ArgumentException("password is null or empty");

            if (configureRequest == null)
            {
                configureRequest = x => { };
            }

            this.username = username;
            this.password = password;

            this.timeout = timeout ?? defaultTimeout;
            this.configureRequest = configureRequest;
            settings = new JsonSerializerOptions
            {
                DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,
                PropertyNameCaseInsensitive = true
            };
        }

        private void TryInitializeApiHostName(string address, bool useSsl)
        {
            string result = $"{address.Trim()}:{portNumber}";

            if (string.IsNullOrEmpty(result)) return;

            string schema = useSsl ? "https://" : "http://";

            result = result.Contains(schema) ? result : schema + result;

            if (UrlRegex.IsMatch(result) && Uri.TryCreate(result, UriKind.Absolute, out _))
            {
                apiAddressCollection.Add(result);
            }
        }

        internal async Task<Vhost> CreateVirtualHostAsync(string virtualHostName)
        {
            if (string.IsNullOrEmpty(virtualHostName)) throw new ArgumentException("virtualHostName is null or empty");

            await PutAsync($"vhosts/{virtualHostName}").ConfigureAwait(false);

            return await GetVhostAsync(virtualHostName).ConfigureAwait(false);
        }

        internal Task<Vhost> GetVhostAsync(string vhostName)
        {
            string vhost = SanitiseVhostName(vhostName);
            return GetAsync<Vhost>($"vhosts/{vhost}");
        }

        internal Task<IEnumerable<Vhost>> GetVHostsAsync()
        {
            return GetAsync<IEnumerable<Vhost>>("vhosts");
        }

        internal async Task CreatePermissionAsync(PermissionInfo permissionInfo)
        {
            if (permissionInfo is null) throw new ArgumentNullException("permissionInfo");

            string vhost = SanitiseVhostName(permissionInfo.GetVirtualHostName());
            string username = permissionInfo.GetUserName();
            await PutAsync($"permissions/{vhost}/{username}", permissionInfo).ConfigureAwait(false);
        }

        internal async Task CreateFederatedExchangeAsync(FederatedExchange exchange, string ownerVhost)
        {
            await PutAsync($"parameters/federation-upstream/{ownerVhost}/{exchange.Name}", exchange).ConfigureAwait(false);
        }

        internal async Task CreatePolicyAsync(Policy policy, string ownerVhost)
        {
            await PutAsync($"policies/{ownerVhost}/{policy.Name}", policy).ConfigureAwait(false);
        }

        internal Task<IEnumerable<User>> GetUsersAsync()
        {
            return GetAsync<IEnumerable<User>>("users");
        }

        internal Task<User> GetUserAsync(string userName)
        {
            return GetAsync<User>(string.Format("users/{0}", userName));
        }

        internal async Task<User> CreateUserAsync(UserInfo userInfo)
        {
            if (userInfo is null) throw new ArgumentNullException("userInfo");

            string username = userInfo.GetName();

            await PutAsync($"users/{username}", userInfo).ConfigureAwait(false);

            return await GetUserAsync(userInfo.GetName()).ConfigureAwait(false);
        }

        private async Task PutAsync(string path)
        {
            var request = CreateRequestForPath(path);
            request.Method = "PUT";
            request.ContentType = "application/json";

            using (var response = (HttpWebResponse)await request.GetResponseAsync().ConfigureAwait(false))
            {
                // The "Cowboy" server in 3.7.0's Management Client returns 201 Created.
                // "MochiWeb/1.1 WebMachine/1.10.0 (never breaks eye contact)" in 3.6.1 and previous return 204 No Content
                // Also acceptable for a PUT response is 200 OK
                // See also http://stackoverflow.com/questions/797834/should-a-restful-put-operation-return-something
                if (!(response.StatusCode == HttpStatusCode.OK ||
                      response.StatusCode == HttpStatusCode.Created ||
                      response.StatusCode == HttpStatusCode.NoContent))
                {
                    throw new UnexpectedHttpStatusCodeException(response.StatusCode);
                }
            }
        }

        private async Task PutAsync<T>(string path, T item)
        {
            var request = CreateRequestForPath(path);
            request.Method = "PUT";

            await InsertRequestBodyAsync(request, item).ConfigureAwait(false);

            using (var response = (HttpWebResponse)await request.GetResponseAsync().ConfigureAwait(false))
            {
                // The "Cowboy" server in 3.7.0's Management Client returns 201 Created.
                // "MochiWeb/1.1 WebMachine/1.10.0 (never breaks eye contact)" in 3.6.1 and previous return 204 No Content
                // Also acceptable for a PUT response is 200 OK
                // See also http://stackoverflow.com/questions/797834/should-a-restful-put-operation-return-something
                if (!(response.StatusCode == HttpStatusCode.OK ||
                      response.StatusCode == HttpStatusCode.Created ||
                      response.StatusCode == HttpStatusCode.NoContent))
                {
                    throw new UnexpectedHttpStatusCodeException(response.StatusCode);
                }
            }
        }

        private async Task<T> GetAsync<T>(string path, params object[] queryObjects)
        {
            var request = CreateRequestForPath(path, queryObjects);

            using (var response = (HttpWebResponse)await request.GetResponseAsync().ConfigureAwait(false))
            {
                if (response.StatusCode != HttpStatusCode.OK)
                {
                    throw new UnexpectedHttpStatusCodeException(response.StatusCode);
                }
                return DeserializeResponse<T>(response);
            }
        }

        private async Task InsertRequestBodyAsync<T>(HttpWebRequest request, T item)
        {
            request.ContentType = "application/json";

            var body = JsonSerializer.Serialize(item, settings);
            using (var requestStream = await request.GetRequestStreamAsync().ConfigureAwait(false))
            using (var writer = new StreamWriter(requestStream))
            {
                writer.Write(body);
            }
        }

        private string SanitiseVhostName(string vhostName) => vhostName.Replace("/", "%2f");

        private T DeserializeResponse<T>(HttpWebResponse response)
        {
            var responseBody = GetBodyFromResponse(response);
            return JsonSerializer.Deserialize<T>(responseBody, settings);
        }

        private static string GetBodyFromResponse(HttpWebResponse response)
        {
            string responseBody;
            using (var responseStream = response.GetResponseStream())
            {
                if (responseStream == null)
                {
                    //throw new EasyNetQManagementException("Response stream was null");
                }
                using (var reader = new StreamReader(responseStream))
                {
                    responseBody = reader.ReadToEnd();
                }
            }
            return responseBody;
        }

        private HttpWebRequest CreateRequestForPath(string path, object[] queryObjects = null)
        {
            var endpointAddress = BuildEndpointAddress(path);
            var queryString = BuildQueryString(queryObjects);

            var uri = new Uri(endpointAddress + queryString);

            if (runningOnMono)
            {
                // unsightly hack to fix path.
                // The default vHost in RabbitMQ is named '/' which causes all sorts of problems :(
                // We need to escape it to %2f, but System.Uri then unescapes it back to '/'
                // The horrible fix is to reset the path field to the original path value, after it's
                // been set.
                var pathField = typeof(Uri).GetField("path", BindingFlags.Instance | BindingFlags.NonPublic);
                if (pathField == null)
                {
                    throw new ApplicationException("Could not resolve path field");
                }
                var alteredPath = (string)pathField.GetValue(uri);
                alteredPath = alteredPath.Replace(@"///", @"/%2f/");
                alteredPath = alteredPath.Replace(@"//", @"/%2f");
                alteredPath = alteredPath.Replace("+", "%2b");
                pathField.SetValue(uri, alteredPath);
            }

            var request = (HttpWebRequest)WebRequest.Create(uri);
            request.Credentials = new NetworkCredential(username, password);
            request.Timeout = request.ReadWriteTimeout = (int)timeout.TotalMilliseconds;
            request.KeepAlive = false; //default WebRequest.KeepAlive to false to resolve spurious 'the request was aborted: the request was canceled' exceptions

            configureRequest(request);

            return request;
        }

        private string BuildEndpointAddress(string path)
        {
            if (string.IsNullOrEmpty(lastKnownApiAddress) == false)
            {
                if (IsHostResponding(lastKnownApiAddress))
                    return string.Format("{0}/api/{1}", lastKnownApiAddress, path);
            }

            foreach (var apiAddress in apiAddressCollection)
            {
                if (IsHostResponding(apiAddress))
                {
                    lastKnownApiAddress = apiAddress;
                    return string.Format("{0}/api/{1}", apiAddress, path);
                }
            }

            throw new Exception("Unable to connect to any of the provided API hosts.");
        }

        private bool IsHostResponding(string address)
        {
            try
            {
                HttpWebRequest myRequest = (HttpWebRequest)WebRequest.Create(address);
                myRequest.Timeout = 1000;
                HttpWebResponse response = (HttpWebResponse)myRequest.GetResponse();

                if (response.StatusCode == HttpStatusCode.OK)
                {
                    response.Close();
                    return true;
                }
                else
                {
                    response.Close();
                    return false;
                }
            }
            catch (Exception)
            {
                return false;
            }
        }

        private string BuildQueryString(object[] queryObjects)
        {
            if (queryObjects == null || queryObjects.Length == 0)
                return string.Empty;

            StringBuilder queryStringBuilder = new StringBuilder("?");
            var first = true;
            // One or more query objects can be used to build the query
            foreach (var query in queryObjects)
            {
                if (query == null)
                    continue;
                // All public properties are added to the query on the format property_name=value
                var type = query.GetType();
                foreach (var prop in type.GetProperties())
                {
                    var name = Regex.Replace(prop.Name, "([a-z])([A-Z])", "$1_$2").ToLower();
                    var value = prop.GetValue(query, null);
                    if (!first)
                    {
                        queryStringBuilder.Append("&");
                    }
                    queryStringBuilder.AppendFormat("{0}={1}", name, value ?? string.Empty);
                    first = false;
                }
            }
            return queryStringBuilder.ToString();
        }
    }
}
