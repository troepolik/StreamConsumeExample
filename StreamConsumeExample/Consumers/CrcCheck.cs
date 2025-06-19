using System.IO.Hashing;
using RabbitMQ.Stream.Client;

namespace sb2.service.oddschangeexport.Consumers;

internal class CrcCheck : ICrc32
{
    public byte[] Hash(byte[] data)
    {
        // Here we use the System.IO.Hashing.Crc32 implementation
        return Crc32.Hash(data);
    }
}
