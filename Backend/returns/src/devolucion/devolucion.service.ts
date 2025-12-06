import {
  Injectable,
  NotFoundException,
  BadRequestException,
  Logger,
} from '@nestjs/common';
import { CreateDevolucionDto } from './dto/create-devolucion.dto';
import { UpdateDevolucionDto } from './dto/update-devolucion.dto';
import { AprobarDevolucionDto } from './dto/aprobar-devolucion.dto';
import { RechazarDevolucionDto } from './dto/rechazar-devolucion.dto';
import { InjectRepository } from '@nestjs/typeorm';
import { Devolucion } from './entities/devolucion.entity';
import { Repository } from 'typeorm';
import { KafkaProducerService } from '../common/kafka/kafkaprovider.service';
import { OrderService } from './order/order.service';
import { EstadoDevolucion } from '../common/enums/estado-devolucion.enum';
import { AccionItemDevolucion } from '../common/enums/accion-item-devolucion.enum';
import { PaymentsService } from '../payments/payments.service';
import { ReembolsoService } from '../reembolso/reembolso.service';
import { InstruccionesDevolucionService } from './services/instrucciones-devolucion.service';
import { DevolucionHistorial } from '../devolucion-historial/entities/devolucion-historial.entity';
import { InstruccionesDevolucion } from './interfaces/instrucciones-devolucion.interface';
import moment from 'moment-timezone';

@Injectable()
export class DevolucionService {
  private readonly logger = new Logger(DevolucionService.name);

  constructor(
    @InjectRepository(Devolucion)
    private readonly devolucionRepository: Repository<Devolucion>,
    @InjectRepository(DevolucionHistorial)
    private readonly historialRepository: Repository<DevolucionHistorial>,
    private readonly orderService: OrderService,
    private readonly kafkaProducerService: KafkaProducerService,
    private readonly paymentsService: PaymentsService,
    private readonly reembolsoService: ReembolsoService,
    private readonly instruccionesService: InstruccionesDevolucionService,
  ) {}

  async create(createDevolucionDto: CreateDevolucionDto) {
    const order = await this.orderService.getOrderById(
      createDevolucionDto.orden_id,
    );

    if (!order) {
      throw new NotFoundException(
        `Order with ID ${createDevolucionDto.orden_id} not found`,
      );
    }

    // --- LÓGICA PARA GENERAR ID LEGIBLE (DEV-YYYYMMDD-XXXXXX) ---

    // 1. Buscar la última devolución para obtener el correlativo
    const lastDevolucion = await this.devolucionRepository.find({
      order: { correlativo: 'DESC' },
      take: 1,
    });

    // 2. Calcular el siguiente número
    const nextCorrelativo = (lastDevolucion[0]?.correlativo || 0) + 1;

    // 3. Generar el string (Ej: DEV-20251128-000001)
    const fechaStr = moment().tz('America/Lima').format('YYYYMMDD');

    const codDevolucion = `DEV-${fechaStr}-${nextCorrelativo.toString().padStart(6, '0')}`;

    // 4. Crear la entidad con los nuevos campos
    const devolucion = this.devolucionRepository.create({
      ...createDevolucionDto,
      codDevolucion: codDevolucion,
      correlativo: nextCorrelativo,
    });

    // 1. GUARDA EN POSTGRES PRIMERO Y ESPERA EL ID ASIGNADO
    await this.devolucionRepository.save(devolucion);
    const savedDevolucion = await this.devolucionRepository.findOne({
      where: { id: devolucion.id },
      relations: ['items', 'historial'],
      order: { historial: { fecha_creacion: 'ASC' } },
    });

    // AGREGAMOS LA VERIFICACIÓN DE NULL
    if (!savedDevolucion) {
      throw new BadRequestException('Error al crear la devolución.');
    }
    if (!savedDevolucion.historial) {
      savedDevolucion.historial = [];
    }

    // 2. REGISTRA EL ESTADO INICIAL EN LA TABLA DE HISTORIAL (Opcional, pero buena práctica)
    const nuevaEntradaHistorial = await this.registrarHistorial(
      savedDevolucion.id,
      null,
      savedDevolucion.estado, // 'solicitado'
      1,
    );
    savedDevolucion.historial.push(nuevaEntradaHistorial);

    // La proyección a MongoDB se maneja vía Kafka en orders-query
    this.logger.log(
      'PAYLOAD DE KAFKA SALIENTE:',
      JSON.stringify(savedDevolucion, null, 2),
    );
    await this.kafkaProducerService.emitReturnCreated({
      eventType: 'return-created',
      data: savedDevolucion,
      timestamp: new Date().toISOString(),
    });
    //return this.devolucionRepository.save(devolucion);
    return savedDevolucion;
  }

  // --- MÉTODO CORREGIDO PARA MOSTRAR DATOS CORRECTOS ---
  async findAll() {
    const devoluciones = await this.devolucionRepository.find({
      relations: ['items'],
      //order: { createdAt: 'DESC' },
    });

    if (!devoluciones || devoluciones.length === 0) return [];

    const devolucionesEnriquecidas = await Promise.all(
      devoluciones.map(async (devolucion) => {
        try {
          const orderDetails: any = await this.orderService.getOrderById(
            devolucion.orden_id,
          );

          // Extracción robusta de datos
          let nombreCliente = 'N/A';
          let codOrden = 'N/A'; // Variable para el código formateado

          if (orderDetails) {
            // Nombre
            if (orderDetails.customerName)
              nombreCliente = orderDetails.customerName;
            else if (orderDetails.direccionEnvio?.nombreCompleto)
              nombreCliente = orderDetails.direccionEnvio.nombreCompleto;
            else if (orderDetails.nombre) nombreCliente = orderDetails.nombre;

            // Código de orden formateado (si existe en el microservicio de ordenes)
            if (orderDetails.cod_orden) codOrden = orderDetails.cod_orden;
            else if (orderDetails.codOrden) codOrden = orderDetails.codOrden;
          }

          const montoTotal = devolucion.items
            ? devolucion.items.reduce(
                //(sum, item) => sum + Number(item.precio_compra) * item.cantidad,
                (sum, item) =>
                  sum + Number(item.precio_unitario_dev) * item.cantidad_dev,
                0,
              )
            : 0;

          let tipoDevolucion = 'Solicitado';
          if (devolucion.items && devolucion.items.length > 0) {
            const tieneReembolso = devolucion.items.some(
              (i) => i.tipo_accion === AccionItemDevolucion.REEMBOLSO,
            );
            const tieneReemplazo = devolucion.items.some(
              (i) => i.tipo_accion === AccionItemDevolucion.REEMPLAZO,
            );
            if (tieneReembolso && tieneReemplazo) tipoDevolucion = 'Mixta';
            else if (tieneReembolso) tipoDevolucion = 'Reembolso';
            else if (tieneReemplazo) tipoDevolucion = 'Reemplazo';
          }

          return {
            ...devolucion,
            nombreCliente,
            codOrden, // Enviamos el código formateado
            montoTotal,
            tipoDevolucion,
          };
        } catch (error) {
          return {
            ...devolucion,
            nombreCliente: 'Error',
            codOrden: 'Error',
            montoTotal: 0,
            tipoDevolucion: 'N/A',
          };
        }
      }),
    );
    return devolucionesEnriquecidas;
  }

  async findOne(id: string) {
    const devolucion = await this.devolucionRepository.findOne({
      where: { id },
      //relations: ['historial', 'items', 'reembolso', 'reemplazo'],
      relations: ['historial', 'items', 'reembolso'],
      order: { historial: { fecha_creacion: 'DESC' } }, // Ordenar historial
    });

    if (!devolucion) throw new NotFoundException(`Devolución ${id} not found`);

    // ENRIQUECER EL DETALLE TAMBIÉN
    try {
      const orderDetails: any = await this.orderService.getOrderById(
        devolucion.orden_id,
      );
      let datosCliente = {
        nombres: 'N/A',
        telefono: 'N/A',
        idUsuario: 'N/A',
      };
      let codOrden = devolucion.orden_id;

      if (orderDetails) {
        // Mapeo de datos del cliente desde la orden
        datosCliente.nombres =
          orderDetails.direccionEnvio?.nombreCompleto ||
          orderDetails.customerName ||
          'N/A';
        datosCliente.telefono = orderDetails.direccionEnvio?.telefono || 'N/A';
        // Asumiendo que el email viene en la orden o usuarioId
        datosCliente.idUsuario = orderDetails.usuarioId || 'N/A';

        // Código de orden formateado
        if (orderDetails.cod_orden) codOrden = orderDetails.cod_orden;
        else if (orderDetails.codOrden) codOrden = orderDetails.codOrden;
      }

      return {
        ...devolucion,
        datosCliente, // Añadimos objeto con datos del cliente
        codOrden, // Añadimos código formateado
      };
    } catch (e) {
      this.logger.warn(
        `No se pudieron cargar detalles extra para devolución ${id}`,
      );
      return devolucion;
    }
  }

  async update(id: string, updateDevolucionDto: UpdateDevolucionDto) {
    const devolucion = await this.findOne(id);
    Object.assign(devolucion, updateDevolucionDto);
    return await this.devolucionRepository.save(devolucion);
  }
  async remove(id: string) {
    const devolucion = await this.findOne(id);
    return await this.devolucionRepository.remove(devolucion);
  }

  // --- REEMBOLSO AUTOMÁTICO (AHORA GUARDA HISTORIAL) ---
  async executeRefund(id: string): Promise<Devolucion> {
    const devolucion = await this.devolucionRepository.findOne({
      where: { id },
      relations: ['items'],
    });
    if (!devolucion) throw new NotFoundException(`Devolución ${id} not found`);

    const estadoAnterior = devolucion.estado;

    // Validación de estado
    if (
      devolucion.estado !== EstadoDevolucion.PROCESANDO &&
      devolucion.estado !== EstadoDevolucion.SOLICITADO
    ) {
      if (devolucion.estado === EstadoDevolucion.COMPLETADO) return devolucion;
      throw new BadRequestException(
        `La devolución debe estar SOLICITADO o PROCESANDO.`,
      );
    }

    // Calcular monto
    const montoTotalReembolso = devolucion.items
      .filter((item) => item.tipo_accion === AccionItemDevolucion.REEMBOLSO)
      .reduce(
        (sum, item) =>
          sum + Number(item.precio_unitario_dev) * item.cantidad_dev,
        0,
      );

    // 1. Cambio a Procesando
    if (devolucion.estado === EstadoDevolucion.SOLICITADO) {
      devolucion.estado = EstadoDevolucion.PROCESANDO;
      await this.devolucionRepository.save(devolucion);
      // Registramos historial de "Iniciando proceso"
      await this.registrarHistorial(
        devolucion.id,
        estadoAnterior,
        EstadoDevolucion.PROCESANDO,
        1,
        //'Iniciando reembolso automático',
      );
    }

    // 2. Llamada a Pagos
    const refundResponse = await this.paymentsService.processRefund({
      orden_id: devolucion.orden_id,
      monto: montoTotalReembolso,
      motivo: `Reembolso para devolución #${devolucion.id}`,
    });

    if (refundResponse && refundResponse.reembolso_id) {
      // 3. ÉXITO
      const nuevoReembolso = await this.reembolsoService.create({
        devolucion_id: devolucion.id,
        monto: montoTotalReembolso,
        fecha_procesamiento: new Date(
          refundResponse.fecha_reembolso,
        ).toISOString(),
        estado: 'procesado',
        transaccion_id: refundResponse.reembolso_id,
        moneda: 'PEN', //esto se deja asi hasta q se fije la manera de obtenerlo del front
        //moneda: devolucion.items[0]?.moneda || 'PEN',
      });

      //devolucion.reembolso_id = nuevoReembolso.id;
      devolucion.estado = EstadoDevolucion.COMPLETADO;
      //devolucion.fecha_procesamiento = new Date();
      await this.devolucionRepository.save(devolucion);

      // --- ¡AQUÍ GUARDAMOS EL HISTORIAL FINAL! ---
      await this.registrarHistorial(
        devolucion.id,
        EstadoDevolucion.PROCESANDO,
        EstadoDevolucion.COMPLETADO,
        1, // ID de sistema/admin
        //`Reembolso procesado exitosamente. TX: ${refundResponse.reembolso_id}`,
      );

      await this.kafkaProducerService.returnPaid({
        devolucionId: devolucion.id,
        reembolsoId: nuevoReembolso.id,
        monto: montoTotalReembolso,
      });
    } else {
      // 4. ERROR
      devolucion.estado = EstadoDevolucion.ERROR_REEMBOLSO;
      await this.devolucionRepository.save(devolucion);

      await this.registrarHistorial(
        devolucion.id,
        EstadoDevolucion.PROCESANDO,
        EstadoDevolucion.ERROR_REEMBOLSO,
        1,
        //'Error al comunicarse con la pasarela de pagos',
      );
    }

    return devolucion;
  }

  async aprobarDevolucion(
    id: string,
    aprobarDto: AprobarDevolucionDto,
  ): Promise<{
    devolucion: Devolucion;
    instrucciones: InstruccionesDevolucion;
  }> {
    const devolucion = await this.findOne(id);

    if (devolucion.estado !== EstadoDevolucion.SOLICITADO) {
      throw new BadRequestException(
        `No se puede aprobar una devolución en estado ${devolucion.estado}`,
      );
    }

    const order: any = await this.orderService.getOrderById(devolucion.orden_id);
    if (!order) {
      throw new NotFoundException(
        `Order with ID ${devolucion.orden_id} not found`,
      );
    }
    //CAMBIO DE ESTADO INICIAL
    const estadoAnterior = devolucion.estado;
    //devolucion.estado = EstadoDevolucion.PROCESANDO;
    devolucion.estado = EstadoDevolucion.APROBADO;
    //devolucion.fecha_procesamiento = new Date();

    // 2. **LÓGICA DE REEMPLAZO/REEMBOLSO**
    
    //2a. Reemplazo: Crear Orden Nueva (Si aplica)
    const itemsReemplazo = devolucion.items.filter(i => i.tipo_accion === AccionItemDevolucion.REEMPLAZO);
    if (itemsReemplazo.length > 0) {
      this.logger.log(`Procesando reemplazo para devolución ${id} con ${itemsReemplazo.length} items`);
      
      try {
        // Validar que todos los items de reemplazo tengan datos completos
        const itemsValidos = itemsReemplazo.every(item => 
          item.producto_id_new && item.cantidad_new && item.precio_unitario_new
        );

        if (!itemsValidos) {
          this.logger.warn(`No se puede crear orden de reemplazo: items incompletos`);
        } else {
          // Construir los items para la nueva orden
          const newOrderItems = itemsReemplazo.map(item => ({
            productoId: item.producto_id_new!,
            nombreProducto: `Producto ${item.producto_id_new}`, // Idealmente obtener del catálogo
            cantidad: item.cantidad_new!,
            precioUnitario: item.precio_unitario_new!,
            subTotal: item.precio_unitario_new! * item.cantidad_new!,
          }));

          // Calcular costos
          const subtotal = newOrderItems.reduce((sum, item) => sum + item.subTotal, 0);
          const envio = order.costos?.envio || 0; // Reutilizar costo de envío original
          const total = subtotal + envio;

          // Crear la orden de reemplazo
          const nuevaOrden = await this.orderService.createReplacementOrder({
            usuarioId: order.usuarioId,
            direccionEnvio: order.direccionEnvio,
            costos: {
              subtotal,
              envio,
              total,
            },
            entrega: order.entrega,
            metodoPago: order.metodoPago || 'REEMPLAZO',
            estadoInicial: 'PENDIENTE',
            items: newOrderItems,
          });

          devolucion.orden_reemplazo_id = nuevaOrden.orden_id;
          this.logger.log(`Orden de reemplazo ${nuevaOrden.orden_id} creada exitosamente`);
        }
      } catch (error) {
        this.logger.error(`Error al crear orden de reemplazo: ${error.message}`, error.stack);
        // No bloqueamos la aprobación si falla la creación del reemplazo
        // Podríamos emitir un evento de error para notificar al admin
      }
    }

    // 2b. Reembolso: Marcar para ingreso de datos de cuenta (Si aplica)
    const itemsReembolso = devolucion.items.filter(
      (i) => i.tipo_accion === AccionItemDevolucion.REEMBOLSO,
    );
    if (itemsReembolso.length > 0) {
      this.logger.log(
        `Devolución ${id} aprobada, requiere datos de reembolso.`,
      );
    }

    const devolucionActualizada =
      await this.devolucionRepository.save(devolucion);

    const instrucciones = await this.instruccionesService.generarInstrucciones(
      devolucionActualizada,
      aprobarDto.metodoDevolucion,
    );
    // 3. **REGISTRAR HISTORIAL** (sin comentario)
    await this.registrarHistorial(
      devolucion.id,
      estadoAnterior,
      EstadoDevolucion.APROBADO,
      aprobarDto.adminId,
    );

    await this.kafkaProducerService.emitReturnApproved({
      eventType: 'return-approved',
      data: {
        devolucionId: devolucion.id,
        orderId: devolucion.orden_id,
        customerId: order.customerId || 'unknown',
        customerName: order.customerName,
        estado: devolucionActualizada.estado,
        //numeroAutorizacion: instrucciones.numeroAutorizacion,
        adminId: aprobarDto.adminId,
        //comentario: aprobarDto.comentario,
      },
      timestamp: new Date().toISOString(),
    });

    await this.kafkaProducerService.emitReturnInstructionsGenerated({
      eventType: 'return-instructions-generated',
      data: {
        devolucionId: devolucion.id,
        orderId: devolucion.orden_id,
        customerId: order.customerId || 'unknown',
        customerName: order.customerName,
        instrucciones,
      },
      timestamp: new Date().toISOString(),
    });

    return {
      devolucion: devolucionActualizada,
      instrucciones,
    };
  }

  async rechazarDevolucion(
    id: string,
    rechazarDto: RechazarDevolucionDto,
  ): Promise<Devolucion> {
    const devolucion = await this.findOne(id);

    if (devolucion.estado !== EstadoDevolucion.SOLICITADO) {
      throw new BadRequestException(
        `No se puede rechazar una devolución en estado ${devolucion.estado}`,
      );
    }

    const order = await this.orderService.getOrderById(devolucion.orden_id);
    if (!order) {
      throw new NotFoundException(
        `Order with ID ${devolucion.orden_id} not found`,
      );
    }

    const estadoAnterior = devolucion.estado;
    devolucion.estado = EstadoDevolucion.CANCELADO;
    //devolucion.fecha_procesamiento = new Date();

    const devolucionActualizada =
      await this.devolucionRepository.save(devolucion);

    //const comentarioCompleto = `Devolución rechazada. Motivo: ${rechazarDto.motivo}${rechazarDto.comentario ? `. ${rechazarDto.comentario}` : ''}`;
    await this.registrarHistorial(
      devolucion.id,
      estadoAnterior,
      EstadoDevolucion.CANCELADO,
      rechazarDto.adminId,
      //comentarioCompleto,
    );

    await this.kafkaProducerService.emitReturnRejected({
      eventType: 'return-rejected',
      data: {
        devolucionId: devolucion.id,
        orderId: devolucion.orden_id,
        customerId: order.customerId || 'unknown',
        customerName: order.customerName,
        estado: devolucionActualizada.estado,
        //motivo: rechazarDto.motivo,
        //comentario: rechazarDto.comentario,
        adminId: rechazarDto.adminId,
      },
      timestamp: new Date().toISOString(),
    });

    return devolucionActualizada;
  }

  private async registrarHistorial(
    devolucionId: string,
    estadoAnterior: EstadoDevolucion | null,
    estadoNuevo: EstadoDevolucion,
    modificadoPorId: number,
    //comentario: string,
  ): Promise<DevolucionHistorial> {
    const historial = this.historialRepository.create({
      devolucion_id: devolucionId,
      estado_anterior: estadoAnterior,
      estado_nuevo: estadoNuevo,
      modificado_por_id: modificadoPorId,
      //comentario,
    });

    return await this.historialRepository.save(historial);
  }
}
