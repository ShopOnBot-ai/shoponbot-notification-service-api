import { createClient } from "redis";

export const redisClient = createClient({
    url: process.env.REDIS_URL
})
redisClient.on('error', err => console.log('Could not be connect to the redis client.', err));
await redisClient.connect()
console.log("Redis connection established..")