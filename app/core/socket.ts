import { createAdapter } from '@socket.io/redis-adapter'
import http from 'http'
import { Server } from 'socket.io'
import { redisClient } from "../core/redis.js"

export let io: Server

export const socket = async (server: http.Server): Promise<void> => {
    const pubClient = redisClient;
    const subClient = pubClient.duplicate()

    pubClient.on('error', (err) => console.error('Redis Pub Client Error:', err));
    subClient.on('error', (err) => console.error('Redis Sub Client Error:', err));

    try {
        if (!pubClient.isOpen || !subClient.isOpen){
            await Promise.all([
                pubClient.isOpen ? Promise.resolve(): pubClient.connect(), 
                subClient.isOpen ? Promise.resolve(): subClient.connect()
            ])
        }
        console.log("Redis Pub/Sub channels successfully mounted for socket scaling.");

        io = new Server(server, {
            cors: {
                origin: ["http://127.0.0.1:8081", "http://localhost:3000"],
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

            socket.on("join_tracking_room", async (token: string) => {
                if (!token) return;
                const redisTrackingKey = `tracking:session:${token}`

                try {
                    const sessionData = await redisClient.get(redisTrackingKey)
                    console.log(sessionData, "session data..")
                    if (!sessionData) {
                        socket.emit("tracking_error", { message: "Tracking session expired or invalid." });
                        return;
                    }
                    const roomName = `room:track:${token}`;
                    await socket.join(roomName);
                    console.log(`Locked Socket [${socket.id}] inside Room: [${roomName}]`);

                    socket.emit("initial_tracking_snapshot", JSON.parse(sessionData));
                } catch (error: any) {
                    console.error("Redis lookup collapsed inside join_tracking_room:", error.message);
                }
            })

            socket.on("driver_location_update", async (data: { token: string, lat: number, lng: number }) => {
                const { token, lat, lng } = data
                if (!token || !lat || !lng) {
                    return;
                }
                const redisTrackingKey = `tracking:session:${token}`

                try {

                    const sessionData = await redisClient.get(redisTrackingKey);
                    if (!sessionData) return;

                    const parsedPayload = JSON.parse(sessionData)
                    parsedPayload.driver_lat = lat;
                    parsedPayload.driver_lng = lng;
                    parsedPayload.updated_at = new Date().toISOString();

                    const currentTTL = await redisClient.ttl(redisTrackingKey)
                    console.log("currentTTL", currentTTL)
                    if (currentTTL > 0) {
                        await redisClient.set(redisTrackingKey, JSON.stringify(parsedPayload), { EX: currentTTL })
                    }

                    const roomTarget = `room:track:${token}`;
                    io.to(roomTarget).emit("location_changed", {
                        driver_lat: lat,
                        driver_lng: lng,
                        updated_at: parsedPayload.updated_at
                    });

                    console.log(`Location Stream -> Token [${token}] -> Lat: ${lat}, Lng: ${lng}`);
                } catch (error: any) {
                    console.error("Driver tracking telemetry broadcast collapsed:", error.message);
                }
            }) 
        });
        console.log("WebSocket Server framework initialized successfully. Listening for secure room bindings...");
    } catch (error) {
        console.error("WebSocket Redis Adapter clustering engine collapsed:", error);
        throw error;
    }
}