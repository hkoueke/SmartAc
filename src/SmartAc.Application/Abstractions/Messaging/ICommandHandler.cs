using MediatR;

namespace SmartAc.Application.Abstractions.Messaging;

internal interface ICommandHandler<in TRequest, TResponse> : IRequestHandler<TRequest, TResponse>
    where TRequest : ICommand<TResponse>
{
}