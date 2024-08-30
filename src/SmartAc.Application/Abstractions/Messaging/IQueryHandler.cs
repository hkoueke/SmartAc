using MediatR;

namespace SmartAc.Application.Abstractions.Messaging;
internal interface IQueryHandler<in TQuery, TResponse> : IRequestHandler<TQuery, TResponse>
    where TQuery : IQuery<TResponse>
{
}