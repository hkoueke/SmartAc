namespace SmartAc.Application.Options;

public sealed class SensorOptions
{
    public decimal TemperatureMin { get; set; }
    public decimal TemperatureMax { get; set; }
    public decimal HumidityPctMin { get; set; }
    public decimal HumidityPctMax { get; set; }
    public decimal CarbonMonoxidePpmMin { get; set; }
    public decimal CarbonMonoxidePpmMax { get; set; }
    public decimal CarbonMonoxideDangerLevel { get; set; }
    public int ReadingAgeInMinutes { get; set; } = 15;

}