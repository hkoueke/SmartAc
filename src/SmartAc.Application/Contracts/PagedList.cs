using Microsoft.EntityFrameworkCore;

namespace SmartAc.Application.Contracts;

public sealed class PagedList<T>
{
    public IEnumerable<T> Items { get; }

    public int Page { get; }

    public int PageSize { get; }

    public int TotalCount { get; }

    public bool HasPreviousPage => Page > 1;

    public bool HasNextPage => Page * PageSize < TotalCount;

    private PagedList(IEnumerable<T> items, int page, int pageSize, int totalCount)
    {
        Items = items;
        Page = page;
        PageSize = pageSize;
        TotalCount = totalCount;
    }

    public static async Task<PagedList<T>> ToPagedListAsync(IQueryable<T> query, int page, int pageSize,
        CancellationToken cancellationToken = default)
    {
        var totalCount = await query.CountAsync(cancellationToken);
        var items = await query.Skip((page - 1) * pageSize).Take(pageSize).ToListAsync(cancellationToken);
        return new PagedList<T>(items, page, pageSize, totalCount);
    }

    public static PagedList<T> ToPagedList(IEnumerable<T> items, int page, int pageSize)
    {
        var listOfItems = items.ToArray();
        return new PagedList<T>(listOfItems, page, pageSize, listOfItems.Length);
    }
}