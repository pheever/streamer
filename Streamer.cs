using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace streamer
{
    abstract class Streamer
    {
        int maxpayload;
        TimeSpan timeout;
        CancellationTokenSource cts;
        BlockingCollection<Dictionary<string, object>> collection;
        public Streamer(int maxpayload, int timeout)
        {
            this.maxpayload = maxpayload;
            this.cts = new CancellationTokenSource();
            this.timeout = TimeSpan.FromMilliseconds(timeout);
            this.collection = new BlockingCollection<Dictionary<string, object>>(maxpayload * 10);
            Task.Run(start, this.cts.Token);
        }
        public abstract Task Ship(List<Dictionary<string, object>> shippable);
        public void Stop() => this.cts.Cancel();
        public void Stop(int timeout) => this.cts.CancelAfter(TimeSpan.FromMilliseconds(timeout));
        public void Stream(Dictionary<string, object> item) => collection.Add(item);
        public void Stream(IEnumerable<Dictionary<string, object>> items)
        {
            foreach (var item in items)
            {
                Stream(item);
            }
        }
        private async Task start()
        {
            Stopwatch sw = new Stopwatch();
            while (true)
            {
                var shippable = await Task.Run(this.Collect);
                Ship(shippable).Forget();
                Console.WriteLine($"Shipped {shippable.Count} items");
            }
        }
        private List<Dictionary<string, object>> Collect()
        {
            List<Dictionary<string, object>> payload = new List<Dictionary<string, object>>(maxpayload);
            CancellationTokenSource cts = new CancellationTokenSource(this.timeout);
            try
            {
                foreach (var t in this.collection.GetConsumingEnumerable(cts.Token))
                {
                    payload.Add(t);
                    if (payload.Count == payload.Capacity)
                    {
                        break;
                    }
                }
            }
            catch (OperationCanceledException) { /* Expected to happen */ }
            catch (Exception e) { Console.WriteLine(e); }
            return payload;
        }
    }
}
