import { Injectable } from "@nestjs/common";
import { HttpService } from "@nestjs/axios";
import { firstValueFrom } from "rxjs";

export interface OrderResponse {
    _id: string;
    num_orden: number;
    cod_orden: string;
    usuarioId: number;
    direccionEnvio: {
        nombreCompleto: string;
        telefono: string;
        direccionLinea1: string;
        direccionLinea2?: string;
        ciudad: string;
        provincia: string;
        codigoPostal: string;
        pais: string;
    };
    costos: {
        subtotal: number;
        envio: number;
        total: number;
    };
    entrega: any;
    metodoPago: string;
    estado: string;
    fechaCreacion: string;
    fechaActualizacion: string;
    items: Array<{
        id: number;
        producto_id: number;
        cantidad: number;
        precioUnitario: number;
        subTotal: number;
        detalle_producto?: any;
    }>;
    historialEstados?: any[];
    tiene_devolucion?: boolean;
    pago?: any;
    id_devolucion?: string;
    // Alias para compatibilidad
    customerId?: string;
    customerName?: string;
    customerEmail?: string;
}

export interface CreateReplacementOrderDto {
    usuarioId: number;
    direccionEnvio: any;
    costos: {
        subtotal: number;
        envio: number;
        total: number;
    };
    entrega: any;
    metodoPago: string;
    estadoInicial: string;
    items: Array<{
        productoId: number;
        nombreProducto: string;
        cantidad: number;
        precioUnitario: number;
        subTotal: number;
    }>;
}

@Injectable()
export class OrderService {
    // --- NUEVA LÍNEA ---
    private readonly ordersApiUrl = process.env.ORDERS_API_URL || 'http://localhost:3002'; // Por defecto, apuntamos a orders-query en el puerto 3002
    private readonly ordersCommandUrl = process.env.ORDERS_COMMAND_URL || 'http://localhost:3000'; // orders-command para crear órdenes

    constructor(private readonly httpClient: HttpService) { }

    async getOrderById(orderId: string): Promise<OrderResponse> {
        // --- LÍNEA MODIFICADA ---
        const url = `${this.ordersApiUrl}/orders/${orderId}`; 
        const response = await firstValueFrom(
            this.httpClient.get<OrderResponse>(url)
        );
        return response.data;
    }

    /**
     * Crea una orden de reemplazo en orders-command
     */
    async createReplacementOrder(createOrderDto: CreateReplacementOrderDto): Promise<any> {
        const url = `${this.ordersCommandUrl}/orders`;
        const response = await firstValueFrom(
            this.httpClient.post(url, createOrderDto)
        );
        return response.data;
    }
}