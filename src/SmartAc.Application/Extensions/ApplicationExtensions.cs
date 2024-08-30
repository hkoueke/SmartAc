namespace SmartAc.Application.Extensions;

public static class ApplicationExtensions
{
    public static bool InRange<T>(this T value, T minValue, T maxValue)
        where T : struct, IComparable, IComparable<T>, IConvertible, IEquatable<T>
    {
        return value.CompareTo(minValue) >= 0 && value.CompareTo(maxValue) <= 0;
    }
}