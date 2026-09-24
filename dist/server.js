import dotenv from "dotenv";
dotenv.config();
console.log("Kafka consumer successfully conected to kafka bootstrap servers", process.env.KAFKA_BOOTSTRAP_SERVERS);
