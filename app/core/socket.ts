import { createAdapter } from '@socket.io/redis-adapter'
import http from 'http'
import { createClient } from 'redis'
import { Server } from 'socket.io'

export let io: Server

export const socket = async(server: http.Server): Promise<void> => {
    const pubClient = createClient({url: process.env.REDIS_URL})
    const subClient = pubClient.duplicate()

    pubClient.on('error', (err) => console.error('Redis Pub Client Error:', err));
    subClient.on('error', (err) => console.error('Redis Sub Client Error:', err));

    try {
        Promise.all([pubClient.connect(), subClient.connect()])
        console.log("Redis Pub/Sub channels successfully mounted for socket scaling.");

        io = new Server(server, {
            cors: {
                origin: "http://127.0.0.1:8081",
                methods: ['GET', 'POST'],
                credentials: true
            }
        })

        io.adapter(createAdapter(pubClient, subClient))

        io.on('connection', (socket) => {
            console.log(`Admin client handshaked successfully. Socket ID: ${socket.id}`);
            
            socket.on('disconnect', () => {
                console.log(`Admin socket channel closed for client: ${socket.id}`);
            });
        });
        console.log("WebSocket Server framework initialized successfully. Listening for secure room bindings...");
    } catch (error) {
        console.error("WebSocket Redis Adapter clustering engine collapsed:", error);
        throw error;
    }
}