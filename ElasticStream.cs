using System;
using System.Collections.Generic;
using System.Security;
using System.Threading.Tasks;
using Nest;

namespace streamer
{
    class ElasticStream : Streamer
    {
        ElasticClient client;
        ConnectionSettings settings;
        public ElasticStream(int maxpayload, int timeout) : base(maxpayload, timeout)
        {
            settings = new ConnectionSettings(new Uri("http://elastic:9200"));
            client = new ElasticClient(settings);
            if (!client.Ping().IsValid)
            {
                throw new Exception("Elasticsearch is unreachable!");
            }
        }

        public override async Task Ship(List<Dictionary<string, object>> shippable)
        {
            var response = await client.IndexManyAsync(shippable, "entries.test");
        }
    }
}