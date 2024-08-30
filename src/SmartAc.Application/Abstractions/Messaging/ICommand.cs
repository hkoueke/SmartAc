using MediatR;

namespace SmartAc.Application.Abstractions.Messaging;

internal interface ICommand<out TResponse> : IRequest<TResponse>
{
}