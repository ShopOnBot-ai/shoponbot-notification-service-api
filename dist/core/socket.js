import { createAdapter } from '@socket.io/redis-adapter';
import { createClient } from 'redis';
import { Server } from 'socket.io';
export let io;
export const socket = async (server) => {
    const pubClient = createClient({ url: process.env.REDIS_URL });
    const subClient = pubClient.duplicate();
    try {
        Promise.all([pubClient.connect(), subClient.connect()]);
        io = new Server(server, {
            cors: {
                origin: "",
                methods: ['GET', 'POST']
            }
        });
        io.adapter(createAdapter(pubClient, subClient));
        io.on('connection', (socket) => {
            console.log(`Admin client handshaked successfully. Socket ID: ${socket.id}`);
            socket.on('disconnect', () => {
                console.log(`Admin socket channel closed for client: ${socket.id}`);
            });
        });
        console.log("WebSocket Server framework initialized successfully. Listening for secure room bindings...");
    }
    catch (error) {
        console.error("WebSocket Redis Adapter clustering engine collapsed:", error);
        throw error;
    }
};
