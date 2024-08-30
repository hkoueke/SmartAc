using MediatR;

namespace SmartAc.Application.Abstractions.Messaging;

internal interface IQuery<out TResponse> : IRequest<TResponse>
{
}