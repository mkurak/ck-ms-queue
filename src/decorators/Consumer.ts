import 'reflect-metadata';
import { ServiceContainer } from 'ck-ms-di';
import { ConsumerOptions, QueueContainer } from '../QueueContainer';

export function consumer(options: ConsumerOptions) {
    return function (target: any, propertyKey: any, descriptor?: PropertyDescriptor) {
        const services = ServiceContainer.getInstance();
        const queue = services.resolve<QueueContainer>(QueueContainer);

        queue.addConsumer(options, descriptor?.value ?? target[propertyKey]);
    };
}
