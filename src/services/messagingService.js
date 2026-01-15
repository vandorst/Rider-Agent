import amqp from 'amqplib';
import {logError, logInfo, logSuccess} from '../utils/logger.js';

export class MessagingService {
    constructor(ip, port, vhost, user, password, sendLogs, isDev) {
        this.ip = ip;
        this.port = port;
        this.vhost = vhost;
        this.user = user;
        this.password = password;
        this.sendLogs = sendLogs;
        this.isDev = isDev;
        this.connections = {};
        this.channels = {};
        this.connectedPromises = {};
    }

    async connect(simRigId, queues = []) {
        if (this.connectedPromises[simRigId]) {
            return this.connectedPromises[simRigId];
        }

        this.connectedPromises[simRigId] = new Promise(async (resolve) => {
            const connectAndSetup = async () => {
                try {
                    const conn = await amqp.connect(`amqp://${this.user}:${this.password}@${this.ip}:${this.port}${this.vhost}`);

                    conn.on('error', (err) => {
                        logError(`RabbitMQ error for SimRig: ${err.message}`);
                    });
                    conn.on('close', () => {
                        logError(`RabbitMQ closed for SimRig, reconnecting...`);
                        this.connectedPromises[simRigId] = null;
                        setTimeout(connectAndSetup, 5000);
                    });

                    const channel = await conn.createChannel();

                    for (const queue of queues) {
                        await channel.assertQueue(queue);
                        logInfo(`Queue ensured [SimRig ${simRigId}] ${queue}`);
                    }

                    this.connections[simRigId] = conn;
                    this.channels[simRigId] = channel;

                    if(this.sendLogs) {
                        logSuccess(`Connected to RabbitMQ Server`);
                    } else {
                        logSuccess(`Connected to RabbitMQ on amqp://${this.ip}:${this.port}`);
                    }

                    resolve();
                } catch (err) {
                    logError(`RabbitMQ Connection Error: ${err.message}`);
                    setTimeout(connectAndSetup, 5000);
                }
            };

            await connectAndSetup();
        });

        return this.connectedPromises[simRigId];
    }

    publish(simRigId, queue, data) {
        const channel = this.channels[simRigId];
        if (!channel) {
            logError(`No RabbitMQ channel available for SimRig to publish to ${queue}`);
            return;
        }
        try {
            channel.sendToQueue(queue, Buffer.from(JSON.stringify(data)));
        } catch (err) {
            if(this.isDev) {
                logError(`Failed to publish message to ${queue}: ${err.message}`);
            }
        }
    }
}
