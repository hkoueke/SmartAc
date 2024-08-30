using SmartAc.Application.Abstractions.Messaging;
using SmartAc.Application.Abstractions.Reporting;
using SmartAc.Application.Contracts;

namespace SmartAc.Application.Features.Devices.Reporting;

public sealed record GetAlertReportsQuery(string SerialNumber, QueryParams Params) : IQuery<PagedList<AlertReport>>;