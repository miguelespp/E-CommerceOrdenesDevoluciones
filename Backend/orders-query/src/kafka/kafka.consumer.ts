import { Injectable } from '@nestjs/common';
import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { MongoService } from '../mongo/mongo.service';

@Controller()
@Injectable()
export class KafkaConsumerService {
  constructor(private readonly mongoService: MongoService) {
    console.log('KafkaConsumerService instanciado');
  }

  @EventPattern('order-created')
  async handleOrderCreated(@Payload() payload: any) {
    await this.replicarOrden(payload.data, 'CREADA');
  }

  @EventPattern('order-cancelled')
  async handleOrderCancelled(@Payload() payload: any) {
    await this.replicarOrden(payload.data, 'CANCELADA');
  }

  @EventPattern('order-paid')
  async handleOrderPaid(@Payload() payload: any) {
    await this.actualizarOrdenPagada(payload.data);
  }

  @EventPattern('order-confirmed')
  async handleOrderConfirmed(@Payload() payload: any) {
    await this.actualizarOrdenConfirmada(payload.data);
  }

  @EventPattern('order-processed')
  async handleOrderProcessed(@Payload() payload: any) {
    await this.actualizarOrdenProcesada(payload.data);
  }

  @EventPattern('order-delivered')
  async handleOrderDelivered(@Payload() payload: any) {
    await this.actualizarOrdenEntregada(payload.data);
  }

  @EventPattern('return-created')
  async handleReturnCreated(@Payload() payload: any) {
    await this.actualizarOrdenConDevolucion(payload.data);
  }

  @EventPattern('return-approved')
  async handleReturnApproved(@Payload() payload: any) {
    await this.actualizarEstadoDevolucion(payload.data);
  }

  @EventPattern('return-rejected')
  async handleReturnRejected(@Payload() payload: any) {
    await this.actualizarEstadoDevolucion(payload.data);
  }

  @EventPattern('return-paid')
  async handleReturnPaid(@Payload() payload: any) {
    await this.actualizarDevolucionPagada(payload);
  }

  @EventPattern('return-instructions-generated')
  async handleReturnInstructionsGenerated(@Payload() payload: any) {
    await this.actualizarInstruccionesDevolucion(payload.data);
  }

  private async replicarOrden(event: any, tipoEvento: 'CREADA' | 'CANCELADA') {
    console.log(
      `Evento de orden ${tipoEvento.toLowerCase()} recibido por Kafka:`,
      event,
    );

    const ordenes = this.mongoService.getCollection('ordenes');
    const historial = event.historialEstados ?? [];

    await ordenes.insertOne({
      _id: event.orden_id,
      num_orden: event.num_orden,
      cod_orden: event.cod_Orden,
      usuarioId: event.clienteId,
      direccionEnvio: event.direccionEnvio,
      costos: event.costos ?? {},
      entrega: event.entrega ?? {},
      metodoPago: event.metodoPago,
      estado: event.estado,
      fechaCreacion: new Date(event.fechaCreacion),
      fechaActualizacion: new Date(event.fechaActualizacion),
      items: event.orden_items ?? [],
      historialEstados: historial,
      tiene_devolucion: false,
    });

    console.log(
      `Orden ${event.orden_id} replicada en order-query como ${tipoEvento}`,
    );
  }

  private async actualizarOrdenPagada(event: any) {
    console.log(`Evento de orden pagada recibido por Kafka:`, event);

    const ordenes = this.mongoService.getCollection('ordenes');

    await ordenes.updateOne(
      { _id: event.orden_id },
      {
        $set: {
          estado: event.estadoNuevo,
          fechaActualizacion: new Date(event.fechaActualizacion),
          pago: {
            pago_id: event.pago.pago_id,
            metodo: event.pago.metodo,
            estado: event.pago.estado,
            fecha_pago: new Date(event.pago.fecha_pago),
            datosPago: event.pago.datosPago,
          },
        },
        $push: {
          historialEstados: {
            estadoAnterior: event.historialNuevo.estadoAnterior,
            estadoNuevo: event.historialNuevo.estadoNuevo,
            fechaModificacion: new Date(event.historialNuevo.fechaModificacion),
            modificadoPor: event.historialNuevo.modificadoPor,
            motivo: event.historialNuevo.motivo,
          },
        },
      },
      { upsert: false },
    );
    console.log(
      `Orden ${event.orden_id} actualizada como PAGADA en order-query`,
    );
  }

  private async actualizarOrdenConfirmada(event: any) {
    console.log(`Evento de orden confirmada recibido por Kafka:`, event);

    const ordenes = this.mongoService.getCollection('ordenes');

    await ordenes.updateOne(
      { _id: event.orden_id },
      {
        $set: {
          estado: event.estadoNuevo,
          fechaActualizacion: new Date(event.fechaActualizacion),
        },
        $push: {
          historialEstados: {
            estadoAnterior: event.historialNuevo.estadoAnterior,
            estadoNuevo: event.historialNuevo.estadoNuevo,
            fechaModificacion: new Date(event.historialNuevo.fechaModificacion),
            modificadoPor: event.historialNuevo.modificadoPor,
            motivo: event.historialNuevo.motivo,
          },
        },
      },
      { upsert: false },
    );

    console.log(
      `Orden ${event.orden_id} actualizada como CONFIRMADA en order-query`,
    );
  }

  private async actualizarOrdenProcesada(event: any) {
    console.log(`Evento de orden procesada recibido por Kafka:`, event);

    const ordenes = this.mongoService.getCollection('ordenes');

    await ordenes.updateOne(
      { _id: event.orden_id },
      {
        $set: {
          estado: event.estadoNuevo,
          fechaActualizacion: new Date(event.fechaActualizacion),
        },
        $push: {
          historialEstados: {
            estadoAnterior: event.historialNuevo.estadoAnterior,
            estadoNuevo: event.historialNuevo.estadoNuevo,
            fechaModificacion: new Date(event.historialNuevo.fechaModificacion),
            modificadoPor: event.historialNuevo.modificadoPor,
            motivo: event.historialNuevo.motivo,
          },
        },
      },
      { upsert: false },
    );

    console.log(
      `Orden ${event.orden_id} actualizada como PROCESADA en order-query`,
    );
  }

  private async actualizarOrdenEntregada(event: any) {
    console.log(`Evento de orden entregada recibido por Kafka:`, event);

    const ordenes = this.mongoService.getCollection('ordenes');

    await ordenes.updateOne(
      { _id: event.orden_id },
      {
        $set: {
          estado: event.estadoNuevo,
          fechaActualizacion: new Date(event.fechaActualizacion),
          evidenciasEntrega: event.evidenciasEntrega ?? [],
        },
        $push: {
          historialEstados: {
            estadoAnterior: event.historialNuevo.estadoAnterior,
            estadoNuevo: event.historialNuevo.estadoNuevo,
            fechaModificacion: new Date(event.historialNuevo.fechaModificacion),
            modificadoPor: event.historialNuevo.modificadoPor,
            motivo: event.historialNuevo.motivo,
          },
        },
      },
      { upsert: false },
    );

    console.log(
      `Orden ${event.orden_id} actualizada como ENTREGADA en order-query`,
    );
  }

  private async actualizarOrdenConDevolucion(event: any) {
    console.log(`Evento de devolución creada recibido por Kafka:`, JSON.stringify(event, null, 2));

    if (!event || !event.orden_id || !event.id) {
      console.error('Evento de devolución con datos insuficientes, ignorando.', {
        hasEvent: !!event,
        hasOrdenId: !!event?.orden_id,
        hasId: !!event?.id,
      });
      return;
    }

    const ordenes = this.mongoService.getCollection('ordenes');
    const devoluciones = this.mongoService.getCollection('devoluciones');
    // --- 1. Actualizar la colección 'ordenes' (para los campos tiene_devolucion/id_devolucion) ---
    await ordenes.updateOne(
      { _id: event.orden_id },
      {
        $set: {
          tiene_devolucion: true,
          id_devolucion: event.id,
        },
      },
      { upsert: false },
    );
    console.log(`Orden ${event.orden_id} marcada con info de devolución.`);

    // --- 2. Crear documento en la colección 'devoluciones' (La Proyección de la Devolución) ---

    const historialProjection = (event.historial || []).map(
      (historialEntry: any) => ({
        //fecha_creacion: new Date(historialEntry.fecha_creacion),
        fecha_creacion: historialEntry.fecha_creacion
          ? new Date(historialEntry.fecha_creacion)
          : null,
        estado_nuevo: historialEntry.estado_nuevo,
        estado_anterior: historialEntry.estado_anterior,
        modificado_por_id: historialEntry.modificado_por_id,
        id_historial: historialEntry.id,
      }),
    );

    console.log('Historial mapeado para MongoDB:', historialProjection);

    // Mapear items correctamente
    const itemsProjection = (event.items || []).map((item: any) => ({
      id: item.id,
      devolucion_id: item.devolucion_id,
      tipo_accion: item.tipo_accion,
      producto_id_dev: item.producto_id_dev,
      precio_unitario_dev: item.precio_unitario_dev,
      cantidad_dev: item.cantidad_dev,
      producto_id_new: item.producto_id_new || null,
      precio_unitario_new: item.precio_unitario_new || null,
      cantidad_new: item.cantidad_new || null,
      motivo: item.motivo || null,
    }));

    console.log('Items mapeados para MongoDB:', itemsProjection);

    await devoluciones.findOneAndUpdate(
      { id: event.id }, // FILTRO: Buscar por el ID de la devolución (que es único)
      {
        $set: {
          id: event.id,
          orden_id: event.orden_id,
          codDevolucion: event.codDevolucion,
          estado: event.estado,
          createdAt: new Date(event.createdAt),
          items: itemsProjection,
          historial: historialProjection,
        },
      },
      {
        upsert: true,
        returnDocument: 'after',
      },
    );
    console.log(`Proyección de devolución ${event.id} creada en order-query.`);
  }

  private async actualizarEstadoDevolucion(event: any) {
    console.log(`Evento de actualización de estado de devolución recibido:`, event);

    if (!event || !event.devolucionId) {
      console.error('Evento de devolución sin devolucionId, ignorando.');
      return;
    }

    const devoluciones = this.mongoService.getCollection('devoluciones');

    await devoluciones.updateOne(
      { id: event.devolucionId },
      {
        $set: {
          estado: event.estado,
        },
      },
      { upsert: false },
    );

    console.log(`Devolución ${event.devolucionId} actualizada con estado: ${event.estado}`);
  }

  private async actualizarDevolucionPagada(event: any) {
    console.log(`Evento de devolución pagada recibido:`, event);

    if (!event || !event.devolucionId) {
      console.error('Evento de devolución pagada sin devolucionId, ignorando.');
      return;
    }

    const devoluciones = this.mongoService.getCollection('devoluciones');

    await devoluciones.updateOne(
      { id: event.devolucionId },
      {
        $set: {
          estado: 'completado',
          reembolso: {
            reembolsoId: event.reembolsoId,
            monto: event.monto,
          },
        },
      },
      { upsert: false },
    );

    console.log(`Devolución ${event.devolucionId} actualizada como pagada con reembolso ${event.reembolsoId}`);
  }

  private async actualizarInstruccionesDevolucion(event: any) {
    console.log(`Evento de instrucciones de devolución generadas recibido:`, event);

    if (!event || !event.devolucionId) {
      console.error('Evento de instrucciones sin devolucionId, ignorando.');
      return;
    }

    const devoluciones = this.mongoService.getCollection('devoluciones');

    await devoluciones.updateOne(
      { id: event.devolucionId },
      {
        $set: {
          instrucciones: event.instrucciones,
        },
      },
      { upsert: false },
    );

    console.log(`Instrucciones agregadas a la devolución ${event.devolucionId}`);
  }
}
