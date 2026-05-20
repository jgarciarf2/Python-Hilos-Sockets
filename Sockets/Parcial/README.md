# Central de Pedidos - Productores y Consumidores

## Descripción

Sistema distribuido en Python que simula una **central de pedidos** usando el patrón **Productor-Consumidor** con comunicación por **sockets TCP/IP** y sincronización mediante **hilos**, **locks**, **semáforos** y **barreras**.

## Arquitectura del Sistema

```
┌─────────────┐     TCP/IP      ┌──────────────────────────────────────────────┐
│  Cliente 1   │ ──────────────→ │                SERVIDOR                      │
│ (Productor)  │                 │                                              │
└─────────────┘                 │   ┌─────────────────────────────────┐        │
                                │   │     Cola Compartida (list)      │        │
┌─────────────┐     TCP/IP      │   │  ┌───┬───┬───┬───┬───┬───┐    │        │
│  Cliente 2   │ ──────────────→ │   │  │ P1│ P2│ P3│ P4│ P5│...│    │        │
│ (Productor)  │                 │   │  └───┴───┴───┴───┴───┴───┘    │        │
└─────────────┘                 │   │  Protegida por: Lock + Semáforo │        │
                                │   └─────────────┬───────────────────┘        │
┌─────────────┐     TCP/IP      │                 │                            │
│  Cliente 3   │ ──────────────→ │        ┌───────┼────────┐                   │
│ (Productor)  │                 │        ▼       ▼        ▼                   │
└─────────────┘                 │   ┌────────┐┌────────┐┌────────┐            │
                                │   │Proces. ││Proces. ││Proces. │            │
     ...más                     │   │   1    ││   2    ││   3    │            │
     clientes                   │   │(Consum)││(Consum)││(Consum)│            │
                                │   └────────┘└────────┘└────────┘            │
                                │         ▲ Sincronizados por BARRERA          │
                                │                                              │
                                │   ┌─────────────────────────────────┐        │
                                │   │   Stock de Productos (dict)     │        │
                                │   │   Protegido por: Lock           │        │
                                │   └─────────────────────────────────┘        │
                                └──────────────────────────────────────────────┘
```

## Componentes del Sistema

### 1. Servidor (`servidor.py`)
- **Hilo principal**: Acepta conexiones TCP de clientes.
- **Hilos operadores** (1 por cliente): Reciben pedidos y los encolan.
- **Hilos procesadores** (3 por defecto): Retiran pedidos de la cola y los despachan.
- **Cola compartida**: Lista Python protegida por Lock.
- **Stock de productos**: Diccionario con inventario, protegido por Lock separado.

### 2. Cliente (`cliente.py`)
- Se conecta al servidor vía socket TCP.
- Recibe la lista de productos disponibles.
- Genera entre 1 y 5 pedidos aleatorios.
- Envía los pedidos y recibe confirmaciones.
- Se desconecta enviando señal de FIN.

## Primitivas de Sincronización Usadas

| Primitiva | Variable | Propósito |
|-----------|----------|-----------|
| **Lock** | `lock_cola` | Protege acceso concurrente a `cola_pedidos` |
| **Lock** | `lock_stock` | Protege acceso concurrente a `stock_productos` |
| **Semaphore** | `semaforo_capacidad` | Limita la cola a máx. 10 pedidos simultáneos |
| **Barrier** | `barrera_procesadores` | Procesadores esperan a que haya ≥5 pedidos antes de empezar |
| **Event** | `evento_barrera_liberada` | Señaliza que la barrera ya fue superada |
| **Event** | `evento_servidor_activo` | Indica si el servidor sigue aceptando clientes |

## Cómo Ejecutar

### Paso 1: Iniciar el servidor
```bash
python servidor.py
```

### Paso 2: Abrir nuevas terminales y ejecutar clientes
```bash
# Terminal 2
python cliente.py

# Terminal 3
python cliente.py

# Terminal 4
python cliente.py

# ... hasta 5 clientes
```

> **Nota**: Cada ejecución de `cliente.py` es un cliente independiente.
> Deben ejecutarse en terminales separadas mientras el servidor está corriendo.

## Protocolo de Comunicación (JSON sobre TCP)

Todos los mensajes se envían como **JSON codificado en UTF-8**.

### Cliente → Servidor

**Pedido:**
```json
{"tipo": "pedido", "producto": "Laptop", "cantidad": 2}
```

**Fin de sesión:**
```json
{"tipo": "fin"}
```

### Servidor → Cliente

**Bienvenida:**
```json
{
    "tipo": "bienvenida",
    "mensaje": "Bienvenido Cliente-1 a la Central de Pedidos",
    "productos_disponibles": ["Laptop", "Mouse", "Teclado", ...],
    "tu_id": "Cliente-1"
}
```

**Confirmación:**
```json
{"tipo": "confirmacion", "mensaje": "Pedido recibido: 2x Laptop. En cola de procesamiento.", "estado": "en_cola"}
```

**Error:**
```json
{"tipo": "error", "mensaje": "Cola llena. No se pudo aceptar el pedido.", "estado": "rechazado"}
```

## Constantes Configurables (en `servidor.py`)

| Constante | Valor | Descripción |
|-----------|-------|-------------|
| `HOST` | `127.0.0.1` | IP donde escucha el servidor |
| `PORT` | `65000` | Puerto TCP |
| `CAPACIDAD_MAXIMA_COLA` | `10` | Máx. pedidos simultáneos en cola (semáforo) |
| `NUM_PROCESADORES` | `3` | Cantidad de hilos consumidores |
| `PEDIDOS_MINIMOS_PARA_BARRERA` | `5` | Pedidos necesarios para liberar la barrera |
| `MAX_CLIENTES` | `5` | Clientes máximos que acepta el servidor |

## Glosario de Conceptos

- **Socket TCP**: Canal de comunicación bidireccional entre dos procesos en red. TCP garantiza entrega ordenada y confiable.
- **Hilo (Thread)**: Unidad de ejecución dentro de un proceso. Comparten memoria.
- **Lock (Mutex)**: Candado que solo un hilo puede tener a la vez. Protege secciones críticas.
- **Semáforo**: Contador protegido que permite hasta N accesos simultáneos.
- **Barrera**: Punto de sincronización donde N hilos esperan hasta que todos lleguen.
- **Productor**: Entidad que genera datos y los coloca en un buffer compartido.
- **Consumidor**: Entidad que retira datos del buffer compartido y los procesa.
- **FIFO**: First In, First Out. El primero que entra es el primero que sale.
- **Sección Crítica**: Bloque de código que accede a recursos compartidos.
- **Condición de Carrera**: Bug donde el resultado depende del orden de ejecución de los hilos.
