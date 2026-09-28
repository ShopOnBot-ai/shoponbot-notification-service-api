import dotenv from "dotenv";
import { kafkaConsumerClient } from "./messaging/consumer.js";
import { createServer } from "http";
import { socket } from "./core/socket.js";
dotenv.config();
console.log("ShopOnBot Node.js Notification Microservice Lifecycle Bootstrap Initiated...");
async function main() {
    const server = createServer((req, res) => {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end("notification service health engine active");
    });
    await socket(server);
    await kafkaConsumerClient.start();
    server.listen(process.env.PORT, () => {
        console.log(`Server is up and running on port: ${process.env.PORT}`);
    });
}
main().catch((err) => {
    console.error("Critical structural microservice bootstrap processing collapse error logs:", err);
    process.exit(1);
});
