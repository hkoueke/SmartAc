using SmartAc.Application.Abstractions.Messaging;
using SmartAc.Application.Abstractions.Reporting;
using SmartAc.Application.Contracts;
using SmartAc.Domain.Alerts;

namespace SmartAc.Application.Features.Devices.Reporting;

internal sealed class GetAlertReportsQueryHandler : IQueryHandler<GetAlertReportsQuery, PagedList<AlertReport>>
{
    private readonly IAlertReportService _alertReportService;

    public GetAlertReportsQueryHandler(IAlertReportService alertReportService)
    {
        _alertReportService = alertReportService;
    }

    public async Task<PagedList<AlertReport>> Handle(GetAlertReportsQuery request, CancellationToken cancellationToken)
    {
        AlertState? alertState = request.Params.State switch
        {
            States.New => AlertState.New,
            States.Resolved => AlertState.Resolved,
            _ => null
        };

        var reports = await _alertReportService.ComputeAlertReportsAsync(request.SerialNumber, alertState, cancellationToken);

        return PagedList<AlertReport>.ToPagedList(reports, request.Params.Page, request.Params.PageSize);
    }
}
