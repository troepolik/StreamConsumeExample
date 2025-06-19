using System.Net;
using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client;
using StreamConsumeExample;

Console.WriteLine("Hello, World!");

string host = "localhost";
string vhost = "";
string queueName = "queueName";
string consumerId = "consumerId";

EndPoint endPoint = new DnsEndPoint(host, 5552);
var addressResolver = new AddressResolver(endPoint);

StreamSystemConfig streamSystemConfig = new StreamSystemConfig()
{
    AddressResolver = addressResolver,
    Endpoints = new List<EndPoint> { addressResolver.EndPoint },
    VirtualHost = vhost,
    Heartbeat = TimeSpan.FromSeconds(120),
};
var logFactory = LoggerFactory.Create(c => c.AddConsole());

ScheduledRabbitStreamConsumer<MessageModel> consumer = new(streamSystemConfig, queueName, logFactory, consumerId);

await consumer.InitAsync();

CancellationToken ct = default;//in real app - liveness token

//main idea is to consume batch of messages and sleep till next iteration
while (!ct.IsCancellationRequested)
{
    await consumer.StartAsync(ct);
    await Task.Delay(TimeSpan.FromMinutes(5));
}
