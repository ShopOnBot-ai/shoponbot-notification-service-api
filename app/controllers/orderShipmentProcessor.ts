import { v7 as randomUUIDv7 } from "uuid"
import twilio from "twilio"
import { redisClient } from "../core/redis.js";

const twilioClient = twilio(
    process.env.TWILIO_ACCOUNT_SID, 
    process.env.TWILIO_AUTH_TOKEN
);

interface EventShippedPayload {
    event_id: number;
    event_type: string;
    aggregate_id: string;
    payload: {
        order_id: number;
        order_number: string;
        user_id: number;
        status: string;
        user_lat: string;
        user_lng: string;
    };
}

export const orderShipmentProcessor = async (eventData: EventShippedPayload) => {
    const orderNum = eventData.payload.order_number
    const userid = eventData.payload.user_id
    const userLat = eventData.payload.user_lat
    const userLng = eventData.payload.user_lng

    if (!userid || !userLat || !userLng) {
        console.error("Incomplete tracking coordinates payload telemetry. Aborting session generation.");
        return;
    }

    try {
        const trackingUrl = randomUUIDv7()
        const redisTrackingKey = `tracking:session:${trackingUrl}`

        const trackingPayload = {
            order_number: orderNum,
            user_id: userid,
            user_lat: parseFloat(userLat),
            user_lng: parseFloat(userLng),
            driver_lat: parseFloat(userLat) + 0.0050,
            driver_lng: parseFloat(userLng) + 0.0050,
            status: "active",
            updated_at: new Date().toISOString()
        };

        await redisClient.set(redisTrackingKey, JSON.stringify(trackingPayload), {EX: 3600})
        console.log(`Redis Memory Lock Established: Token key '${redisTrackingKey}' initialized.`);

        const liveTrackingUrl = `http://localhost:3000/track/${trackingUrl}`;

        const messageBodyData = `Hey Amrit, your ShopOnBot Order #${orderNum} has been successfully SHIPPED! Track your Order here: ${liveTrackingUrl} (Secure link expires in 1 Hour)`;

        await twilioClient.messages.create({
            body: messageBodyData,
            from: process.env.TWILIO_WHATSAPP_NUMBER,
            to: 'whatsapp:+918292019996'
        })

    } catch (streamError: any) {
        console.error("Fail: Notification consumer encountered a collapse inside OrderShipped loop:", streamError.message);
        throw streamError;
    }
}