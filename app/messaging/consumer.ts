import { Kafka, Consumer } from "kafkajs"
import { orderShipmentProcessor } from "../controllers/orderShipmentProcessor.js";
import { orderPaidProcessor } from "../controllers/orderPaidProcessor.js";

const backend_api_url = process.env.BACKEND_API_URL ?? "http://fastapi_app:8000"

class KafkaConsumerManager {
    private kafka: Kafka;
    private consumer: Consumer | null = null

    constructor() {
        this.kafka = new Kafka({
            clientId: 'shoponbot-notification-client',
            brokers: [process.env.KAFKA_BOOTSTRAP_SERVERS || 'kafka_broker:29092'],
            retry: {
                initialRetryTime: 1000,
                retries: 10,           
                factor: 2               
            }
        })
    }
    async start(): Promise<void> {
        try {
            console.log("Connecting Notification Engine to Apache Kafka Broker...");
            this.consumer = this.kafka.consumer({groupId: 'notification-service-group', sessionTimeout: 30000,  heartbeatInterval: 10000})

            console.log("Waiting 5 seconds for Kafka Group Coordinator initialization safety buffer...");
            await new Promise(resolve => setTimeout(resolve, 5000));

            await this.consumer.connect()
            console.log("Kafka Consumer connected successfully. Subscribing to topic...");

            await this.consumer.subscribe({topic: "order-events", fromBeginning: true})
            console.log(`Successfully subscribed to topic: order-events. Listening for stream events...`);

            await this.consumer.run({
                eachMessage: async ({topic, partition, message}) => {
                    if (!message.value) return;

                    const rawPayload = message.value.toString()
                    console.log(rawPayload, "raw payload")
                    const eventData = JSON.parse(rawPayload)
                    console.log(eventData, "event data")

                    console.log(`\n [EVENT CAPTURED] Incoming stream on topic [${topic}]:`);
                    console.log("Event Action Type Type:", eventData.event_type);
                    console.log("Event Core Payload Data Object:", eventData.payload);

                    switch (eventData.event_type) {
                        case "OrderCreated":
                            console.log(`Sending Welcome Invoice email for Order #${eventData.payload.order_number}...`);
                            break;
                    
                        case "OrderPaid":
                            console.log(`Payment Successful alert generated for Order #${eventData.payload.order_number}! Notifiying in whatsapp...`);
                            await orderPaidProcessor(eventData, backend_api_url);
                            break;
                        case "OrderShipped":
                            await orderShipmentProcessor(eventData)
                            break;

                        default:
                            console.log(`Unhandled custom runtime tracking event type string: ${eventData.event_type}`);
                    }
                }
            })
        } catch (error) {
            console.error("Kafka Event Consumer loop system initialization crash failure logs:", error);
            throw error;
        }
    }
    async stop(): Promise<void> {
        if (this.consumer) {
            console.log("Gracefully disconnecting Apache Kafka Consumer connections...");
            await this.consumer.disconnect();
            console.log("Kafka consumer connection terminated safely.");
        }
    }
}

export const kafkaConsumerClient = new KafkaConsumerManager()