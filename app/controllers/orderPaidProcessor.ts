import axios from "axios";
import { io } from "../core/socket.js";

interface OrderItemPayload {
    product_id: number;
    title: string;
    quantity: number;
}

interface EventPaidPayload {
    event_id: number;
    event_type: string;
    aggregate_id: string;
    payload: {
        user_id: number;
        order_id: number;
        order_number: string;
        total_amount: number;
        items: OrderItemPayload[];
    };
}

export const orderPaidProcessor = async (eventData: EventPaidPayload, backend_api_url: string) => {
    if (io) {
        console.log("if called...")
        io.emit('newOrder', {
            event: "OrderPaid",
            order_id: eventData.payload.order_id,
            order_number: eventData.payload.order_number,
            total_amount: eventData.payload.total_amount,
            items_count: eventData.payload.items?.length || 0
        })
    }
    const userId = eventData.payload.user_id;
    console.log("userId", userId)
    if (userId) {
        try {
            await axios.delete(`${backend_api_url}/api/v1/cart/internal/${userId}`)
            console.log(`Success: Backend API executed cart database cleanup for user: ${userId}`);
        } catch (cartError: any) {
            console.error("Non-blocking error: Server-to-server cart clear hook failed:", cartError.message);
            throw cartError;
        }
    }
}