import 'reflect-metadata';
import { ServiceContainer } from 'ck-ms-di';
import { ConsumerOptions, QueueContainer } from '../QueueContainer';

export function Consumer(options: ConsumerOptions) {
    return function (target: any, propertyKey: any, descriptor?: PropertyDescriptor) {
        (async () => {
            const services = ServiceContainer.getInstance();
            const queue = await services.resolveAsync<QueueContainer>(QueueContainer);
            const handler = descriptor?.value ?? target[propertyKey];

            queue?.addConsumer(options, handler);
        })();
    };
}
