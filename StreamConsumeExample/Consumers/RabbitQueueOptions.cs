namespace sb2.service.oddschangeexport.Senders.RabbitMq.Config;
internal record RabbitQueueOptions
{

    public string Queue { get; set; } = "";
    public string? BindExchange { get; set; }
};

