using System.Text.Json;
using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

namespace StreamConsumeExample;
internal class ScheduledRabbitStreamConsumer<TMessage>
    (StreamSystemConfig streamSystemConfig, string queue
    //, IStreamChangeProcessor<TMessage> processor/
    , ILoggerFactory loggerFactory
    , string consumerId)
{
    private readonly StreamSystemConfig _streamSystemConfig = streamSystemConfig;
    //private readonly IStreamChangeProcessor<TMessage, TValidationContext> _processor = processor;

    readonly string _queue = queue;
    readonly string _consumerId = consumerId;
    private StreamSystem _streamSystem = null!;
    private ulong _offsetToProcess;
    private ulong _lastProcessedOffset = 0;
    private CancellationToken _applicationCancellationToken = default;
    private TaskCompletionSource _consumingCompletionSource = new();
    readonly ILogger<ScheduledRabbitStreamConsumer<TMessage>> _logger = loggerFactory.CreateLogger<ScheduledRabbitStreamConsumer<TMessage>>();
    readonly ILogger<StreamSystem> _streamLogger = loggerFactory.CreateLogger<StreamSystem>();
    readonly ILogger<Consumer> _consumerLogger = loggerFactory.CreateLogger<Consumer>();


    Consumer? _consumer;
    private int _processedCount;

    public async Task StartAsync(CancellationToken ct)
    {
        _applicationCancellationToken = ct;
        _consumingCompletionSource = new();//we will release it when stop consuming        

        _consumer = await Consumer.Create(
            new ConsumerConfig(
                _streamSystem,
                _queue)
            {
                ClientProvidedName = "test",//_consumerId,
                Reference = "test",//_consumerId,
                Identifier = "test",//_consumerId,
                OffsetSpec = _offsetToProcess > 0 ? new OffsetTypeOffset(_offsetToProcess) : new OffsetTypeLast(),
                MessageHandler = ProcessMessageAsync,
                Crc32 = new CrcCheck(),
            }
            , _consumerLogger
        ).WaitAsync(ct);

        await _consumingCompletionSource.Task.WaitAsync(ct);

    }


    async Task ProcessMessageAsync(string stream, RawConsumer consumer, MessageContext context, Message message)
    {
        try
        {
            if (_applicationCancellationToken.IsCancellationRequested)
            {
                await Close();
                return;
            }
            if (_lastProcessedOffset == context.Offset)
                return;//skip double process after sleep

            Utf8JsonReader reader = new(message.Data.Contents);
            var msg = JsonSerializer.Deserialize<TMessage>(ref reader);
            if (msg == null)
                return;

            //do some work
            //await _processor.ProcessAsync(msg, validContext, default);

            _lastProcessedOffset = context.Offset;
            _processedCount++;

            //by some condition save offset
            if (_processedCount % 200 == 0)//in real app condition is different
            {
                try
                {
                    await SaveWorkAndStoreOffsetAsync(0, consumer);
                }
                catch (Exception exSave)
                {
                    _logger.LogError(exSave, $"Error save process {exSave.Message} consumer {_consumerId} will paused till next iteration");
                    //if error we will pause consuming and retry later with old offset
                    await Close();
                    return;
                }
            }

            //by some condition exit from consuming
            if (_processedCount % 2000 == 0)//in real app condition is different
            {
                await SaveWorkAndStoreOffsetAsync(context.Offset, consumer);

                await Close();
                return;
            }

        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"{_consumerId} Error process message {context.Offset}.Retry:0 Sleep and retry. {ex.Message}");
            await Close();
        }
    }

    private async Task SaveWorkAndStoreOffsetAsync(ulong currentOffset, RawConsumer rawConsumer)
    {
        //await _processor.SaveWorkAsync();
        _offsetToProcess = currentOffset;
        await rawConsumer.StoreOffset(currentOffset);

    }

    public async Task InitAsync()
    {
        _streamSystem = await StreamSystem.Create(_streamSystemConfig, _streamLogger);

        if (!await _streamSystem.StreamExists(_queue))
        {
            var streamConfig = new StreamSpec(_queue)
            {
                MaxAge = TimeSpan.FromDays(7),//7 days retantion
                MaxLengthBytes = 500_000_000,//500 mb retantion
                MaxSegmentSizeBytes = 20_000_000 // 20 mb per segment                
            };

            await _streamSystem.CreateStream(streamConfig);

        }
        else
        {
            _offsetToProcess = await QueryOffsetAsync(_streamSystem);
        }

    }

    private async Task Close()
    {
        _consumerLogger.LogInformation($"Consumer {_consumerId} will sleep til next iteration");

        if (_consumer != null)
            await _consumer.Close();

        _consumingCompletionSource.TrySetResult();//release awaiters
    }


    private async Task<ulong> QueryOffsetAsync(StreamSystem streamSystem)
    {
        try
        {
            return await streamSystem.QueryOffset(_consumerId, _queue) + 1;
        }
        catch (OffsetNotFoundException)
        {
            return 0;
        }
    }

}
