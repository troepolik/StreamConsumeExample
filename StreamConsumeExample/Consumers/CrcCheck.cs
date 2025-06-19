using System.IO.Hashing;
using RabbitMQ.Stream.Client;

namespace StreamConsumeExample;

internal class CrcCheck : ICrc32
{
    public byte[] Hash(byte[] data)
    {
        // Here we use the System.IO.Hashing.Crc32 implementation
        return Crc32.Hash(data);
    }
}
