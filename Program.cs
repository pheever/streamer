using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace streamer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var strm = new ElasticStream(1000, 500);
            await Task.Run(() =>
            {
                var rand = new Random(DateTime.Now.Millisecond);
                for (var i = 0; i < 1000000; i += 1)
                {
                    Thread.Sleep(rand.Next(2));
                    strm.Stream(new Dictionary<string, object> { });
                }
            });
        }
    }
}
