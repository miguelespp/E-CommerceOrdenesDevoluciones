import {
  Controller,
  Get,
  Post,
  Body,
  Patch,
  Param,
  Delete,
  ParseUUIDPipe,
  HttpCode,
  HttpStatus,
} from '@nestjs/common';
import {
  ApiTags,
  ApiOperation,
  ApiResponse,
  ApiParam,
  ApiBody,
  ApiExtraModels,
} from '@nestjs/swagger';
import { DevolucionService } from './devolucion.service';
import { CreateDevolucionDto } from './dto/create-devolucion.dto';
import { UpdateDevolucionDto } from './dto/update-devolucion.dto';
import { AprobarDevolucionDto } from './dto/aprobar-devolucion.dto';
import { RechazarDevolucionDto } from './dto/rechazar-devolucion.dto';
import { AprobarDevolucionResponseDto } from './dto/aprobar-devolucion-response.dto';
import { Devolucion } from './entities/devolucion.entity';
import { DevolucionHistorial } from '../devolucion-historial/entities/devolucion-historial.entity';
import { ItemDevolucion } from '../items-devolucion/entities/items-devolucion.entity';
import { Reembolso } from '../reembolso/entities/reembolso.entity';
import { Reemplazo } from '../reemplazo/entities/reemplazo.entity';

@ApiTags('Devoluciones')
@ApiExtraModels(
  Devolucion,
  DevolucionHistorial,
  ItemDevolucion,
  Reembolso,
  Reemplazo,
)
@Controller('devolucion')
export class DevolucionController {
  constructor(private readonly devolucionService: DevolucionService) {}

  @Post(':id/refund')
  @ApiOperation({ summary: 'Ejecutar el reembolso de una devolución aprobada' })
  executeRefund(@Param('id', ParseUUIDPipe) id: string) {
    return this.devolucionService.executeRefund(id);
  }

  @Post()
  @ApiOperation({
    summary: 'Crear una nueva solicitud de devolución',
    description:
      'Crea una nueva solicitud de devolución asociada a una orden existente. ' +
      'Verifica que la orden exista y emite un evento de Kafka "return-created".',
  })
  @ApiBody({
    type: CreateDevolucionDto,
    description: 'Datos de la nueva devolución',
    examples: {
      ejemplo1: {
        value: {
          orderId: '550e8400-e29b-41d4-a716-446655440000',
          estado: 'pendiente',
        },
      },
    },
  })
  @ApiResponse({
    status: 201,
    description: 'Devolución creada exitosamente',
    type: Devolucion,
  })
  @ApiResponse({
    status: 400,
    description: 'Datos inválidos o formato incorrecto',
    schema: {
      type: 'object',
      properties: {
        statusCode: { type: 'number', example: 400 },
        message: {
          type: 'array',
          items: { type: 'string' },
          example: ['orderId must be a UUID'],
        },
        error: { type: 'string', example: 'Bad Request' },
      },
    },
  })
  @ApiResponse({
    status: 404,
    description: 'Orden no encontrada',
    schema: {
      type: 'object',
      properties: {
        statusCode: { type: 'number', example: 404 },
        message: {
          type: 'string',
          example:
            'Order with ID 550e8400-e29b-41d4-a716-446655440000 not found',
        },
        error: { type: 'string', example: 'Not Found' },
      },
    },
  })
  create(@Body() createDevolucionDto: CreateDevolucionDto) {
    return this.devolucionService.create(createDevolucionDto);
  }

  @Get()
  @ApiOperation({
    summary: 'Obtener todas las devoluciones',
    description:
      'Retorna una lista completa de todas las devoluciones con sus relaciones: ' +
      'historial, items, reembolso y reemplazo.',
  })
  @ApiResponse({
    status: 200,
    description: 'Lista de todas las devoluciones con sus relaciones',
    type: Devolucion,
    isArray: true,
  })
  findAll() {
    return this.devolucionService.findAll();
  }

  @Get(':id')
  @ApiOperation({
    summary: 'Obtener una devolución por ID',
    description:
      'Retorna una devolución específica con todas sus relaciones: historial, items, reembolso y reemplazo.',
  })
  @ApiParam({
    name: 'id',
    description: 'ID único de la devolución',
    type: String,
    format: 'uuid',
    example: '550e8400-e29b-41d4-a716-446655440000',
  })
  @ApiResponse({
    status: 200,
    description: 'Devolución encontrada con todas sus relaciones',
    type: Devolucion,
  })
  @ApiResponse({
    status: 404,
    description: 'Devolución no encontrada',
    schema: {
      type: 'object',
      properties: {
        statusCode: { type: 'number', example: 404 },
        message: {
          type: 'string',
          example: 'Devolución 550e8400-e29b-41d4-a716-446655440000 not found',
        },
        error: { type: 'string', example: 'Not Found' },
      },
    },
  })
  findOne(@Param('id', ParseUUIDPipe) id: string) {
    return this.devolucionService.findOne(id);
  }

  @Patch(':id')
  @ApiOperation({
    summary: 'Actualizar una devolución',
    description:
      'Actualiza parcialmente los datos de una devolución existente. ' +
      'Si se actualiza el orderId, verifica que la nueva orden exista.',
  })
  @ApiParam({
    name: 'id',
    description: 'ID único de la devolución a actualizar',
    type: String,
    format: 'uuid',
  })
  @ApiBody({
    type: UpdateDevolucionDto,
    description: 'Campos a actualizar (todos opcionales)',
  })
  @ApiResponse({
    status: 200,
    description: 'Devolución actualizada exitosamente',
    type: Devolucion,
  })
  @ApiResponse({
    status: 404,
    description: 'Devolución u orden no encontrada',
    schema: {
      type: 'object',
      properties: {
        statusCode: { type: 'number', example: 404 },
        message: {
          type: 'string',
          example: 'Devolución 550e8400-e29b-41d4-a716-446655440000 not found',
        },
        error: { type: 'string', example: 'Not Found' },
      },
    },
  })
  update(
    @Param('id', ParseUUIDPipe) id: string,
    @Body() updateDevolucionDto: UpdateDevolucionDto,
  ) {
    return this.devolucionService.update(id, updateDevolucionDto);
  }

  @Delete(':id')
  @HttpCode(HttpStatus.NO_CONTENT)
  @ApiOperation({
    summary: 'Eliminar una devolución',
    description:
      'Elimina permanentemente una devolución del sistema. ' +
      'Esta operación no se puede deshacer.',
  })
  @ApiParam({
    name: 'id',
    description: 'ID único de la devolución a eliminar',
    type: String,
    format: 'uuid',
  })
  @ApiResponse({
    status: 204,
    description:
      'Devolución eliminada exitosamente (sin contenido en la respuesta)',
  })
  @ApiResponse({
    status: 404,
    description: 'Devolución no encontrada',
    schema: {
      type: 'object',
      properties: {
        statusCode: { type: 'number', example: 404 },
        message: {
          type: 'string',
          example: 'Devolución 550e8400-e29b-41d4-a716-446655440000 not found',
        },
        error: { type: 'string', example: 'Not Found' },
      },
    },
  })
  remove(@Param('id', ParseUUIDPipe) id: string) {
    return this.devolucionService.remove(id);
  }

  @Patch(':id/aprobar')
  @ApiOperation({
    summary: 'Aprobar una solicitud de devolución',
    description:
      'Aprueba una devolución pendiente, genera instrucciones de devolución y envía notificación al cliente. ' +
      'La devolución debe estar en estado PENDIENTE para poder ser aprobada. ' +
      'Se registra el cambio en el historial y se emiten eventos de Kafka para notificaciones.',
  })
  @ApiParam({
    name: 'id',
    description: 'ID único de la devolución a aprobar',
    type: String,
    format: 'uuid',
  })
  @ApiBody({
    type: AprobarDevolucionDto,
    description:
      'Datos de aprobación incluyendo ID del admin, comentario y método de devolución',
  })
  @ApiResponse({
    status: 200,
    description: 'Devolución aprobada exitosamente con instrucciones generadas',
    type: AprobarDevolucionResponseDto,
  })
  @ApiResponse({
    status: 400,
    description:
      'La devolución no está en estado PENDIENTE o los datos son inválidos',
    schema: {
      type: 'object',
      properties: {
        statusCode: { type: 'number', example: 400 },
        message: {
          type: 'string',
          example: 'No se puede aprobar una devolución en estado procesando',
        },
        error: { type: 'string', example: 'Bad Request' },
      },
    },
  })
  @ApiResponse({
    status: 404,
    description: 'Devolución u orden no encontrada',
    schema: {
      type: 'object',
      properties: {
        statusCode: { type: 'number', example: 404 },
        message: {
          type: 'string',
          example: 'Devolución 550e8400-e29b-41d4-a716-446655440000 not found',
        },
        error: { type: 'string', example: 'Not Found' },
      },
    },
  })
  aprobar(
    @Param('id', ParseUUIDPipe) id: string,
    @Body() aprobarDto: AprobarDevolucionDto,
  ) {
    return this.devolucionService.aprobarDevolucion(id, aprobarDto);
  }

  @Patch(':id/rechazar')
  @ApiOperation({
    summary: 'Rechazar una solicitud de devolución',
    description:
      'Rechaza una devolución pendiente y envía notificación al cliente con el motivo del rechazo. ' +
      'La devolución debe estar en estado PENDIENTE. Se cambia el estado a CANCELADA y se registra en el historial. ' +
      'Se emite un evento de Kafka para notificar al cliente.',
  })
  @ApiParam({
    name: 'id',
    description: 'ID único de la devolución a rechazar',
    type: String,
    format: 'uuid',
  })
  @ApiBody({
    type: RechazarDevolucionDto,
    description:
      'Datos del rechazo incluyendo ID del admin, motivo y comentario opcional',
  })
  @ApiResponse({
    status: 200,
    description: 'Devolución rechazada exitosamente',
    type: Devolucion,
  })
  @ApiResponse({
    status: 400,
    description:
      'La devolución no está en estado PENDIENTE o los datos son inválidos',
    schema: {
      type: 'object',
      properties: {
        statusCode: { type: 'number', example: 400 },
        message: {
          type: 'string',
          example: 'No se puede rechazar una devolución en estado procesando',
        },
        error: { type: 'string', example: 'Bad Request' },
      },
    },
  })
  @ApiResponse({
    status: 404,
    description: 'Devolución u orden no encontrada',
    schema: {
      type: 'object',
      properties: {
        statusCode: { type: 'number', example: 404 },
        message: {
          type: 'string',
          example: 'Devolución 550e8400-e29b-41d4-a716-446655440000 not found',
        },
        error: { type: 'string', example: 'Not Found' },
      },
    },
  })
  rechazar(
    @Param('id', ParseUUIDPipe) id: string,
    @Body() rechazarDto: RechazarDevolucionDto,
  ) {
    return this.devolucionService.rechazarDevolucion(id, rechazarDto);
  }
}
