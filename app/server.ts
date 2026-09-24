import dotenv from "dotenv"
import { kafkaConsumerClient } from "./messaging/consumer.js"

dotenv.config()

console.log("ShopOnBot Node.js Notification Microservice Lifecycle Bootstrap Initiated...");

async function main() {
    await kafkaConsumerClient.start()
}

main().catch((err) => {
    console.error("Critical structural microservice bootstrap processing collapse error logs:", err);
    process.exit(1);
})