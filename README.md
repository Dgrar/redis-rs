# codecrafters-redis

Servidor TCP que implementa un subconjunto de Redis sobre RESP2: `PING`, `ECHO`, `SET` (con expiración `PX`/`EX`) y `GET`. El objetivo es entender parsing binario, I/O asíncrono y estado compartido sin reutilizar la implementación de referencia.

Reto «Build your own Redis» de CodeCrafters, evidenciado por `codecrafters.yml` y los scripts de `.codecrafters/`.

## Estado y funcionalidades actuales

Prototipo funcional en desarrollo. Compila con Rust edición 2024 y los 21 tests unitarios pasan con `cargo test`.

- **`PING`**: responde `+PONG`; los argumentos adicionales se ignoran.
- **`ECHO`**: devuelve el argumento recibido y exige exactamente uno.
- **`SET`**: `SET key value [PX ms | EX s]`; guarda el par y responde `+OK`. Rechaza tiempos de expiración menores o iguales a cero y opciones distintas de `PX`/`EX`.
- **`GET`**: devuelve el valor como `BulkString` o `$-1` si la clave no existe o ya expiró.
- **Parser RESP2**: acepta strings simples, errores, enteros, bulk strings (incluidos nulos y binarios) y arrays anidados (`src/parser.rs`).
- **Pipelining**: procesa todos los mensajes completos acumulados en el buffer antes de volver a leer del socket (`src/main.rs:52-67`).
- **Concurrencia**: una tarea de Tokio por conexión y un `HashMap` global protegido por `RwLock` (`src/main.rs:21-35`).
- **Verificación**: tests unitarios de parsing, serialización, comandos y expiración en los módulos de `src/`.

Limitaciones conocidas:
- Solo existen cuatro comandos; no hay `DEL`, `EXISTS`, `INFO`, replicación, persistencia, Pub/Sub ni transacciones.
- La expiración es perezosa: las claves vencidas se filtran en `GET` pero no se eliminan del `HashMap` hasta que se sobrescriben.
- La dirección y el puerto están fijos en `127.0.0.1:6379`; no se leen flags de CLI ni variables de entorno.

## Requisitos y ejecución rápida

- Rust 1.85+ (edición 2024). El repositorio fija `rust-1.96` en `codecrafters.yml`; verificado con `rustc 1.98.1` en Windows.
- No requiere servicios externos. El servidor escucha en `127.0.0.1:6379`.
- Cliente RESP opcional para el ejemplo: `redis-cli` (paquete `redis-tools`).

```bash
# compilar
cargo build --release

# ejecutar los 21 tests unitarios
cargo test

# arrancar el servidor (bloquea la terminal)
cargo run --release
```

En entornos Unix, `./your_program.sh` reproduce la compilación a `/tmp` y el arranque que usa CodeCrafters.

## Ejemplo de uso real

Con el servidor en marcha, desde otra terminal (cualquier cliente RESP sirve; aquí se usa `redis-cli`):

```bash
redis-cli -p 6379 PING
redis-cli -p 6379 SET clave valor
redis-cli -p 6379 GET clave
redis-cli -p 6379 SET temporal valor PX 100
sleep 0.2
redis-cli -p 6379 GET temporal
```

Salida esperada, derivada de `src/commands.rs` y de los tests unitarios (no capturada de una sesión interactiva):

```text
PONG
OK
"valor"
OK
(nil)
```

El servidor imprime en stdout cada conexión aceptada y cerrada (`src/main.rs:26` y `src/main.rs:34`).

## Decisiones de arquitectura y aprendizaje

- **Concurrencia**: el bucle principal hace `accept().await` y lanza `tokio::spawn` por cliente (`src/main.rs:23-35`). Cada conexión tiene su propio `BytesMut`, así que el estado mutable solo se comparte a través del almacén. El coste es que no hay límite de conexiones, backpressure ni apagado ordenado.
- **Estado compartido**: `Storage` es `Arc<RwLock<HashMap<Vec<u8>, Entry>>>` (`src/storage.rs:5`). `SET` toma el lock de escritura y `GET` el de lectura, lo que permite lecturas concurrentes; a cambio, un lock global serializa las escrituras y bloquea todas las lecturas mientras dura una escritura.
- **Claves y valores binarios**: se usan `Vec<u8>` en lugar de `String` (`src/storage.rs:5-10`), de modo que el servidor no impone UTF-8 ni pierde bytes. El parser conserva los `\r\n` internos de un bulk string, cubierto por el test `parse_bulk_string`. El compromiso es que la API interna no ofrece garantías de texto y cada respuesta clona el valor (`src/commands.rs:140`).
- **Parsing incremental**: `parse_message` recibe un `&[u8]` y devuelve `(RespValue, bytes_consumidos)` o `None` si el mensaje está incompleto (`src/parser.rs:3`). `main` avanza el buffer con `Buf::advance` y repite el parseo, lo que habilita pipelining y mensajes partidos entre paquetes TCP. El coste es que cada lectura vuelve a escanear desde el inicio del buffer y copia el cuerpo de los bulk strings; no hay zero-copy.
- **Errores de protocolo**: el parser usa `Result<_, String>` y un fallo de sintaxis se serializa como error RESP y cierra la conexión (`src/main.rs:61-65`). Los errores de comando, en cambio, viajan como `RespValue::Error` sin cortar la conexión. No hay un enum de errores tipado; `anyhow` se usa solo para los fallos de I/O en `main`.
- **Expiración perezosa**: `Entry` guarda `Option<SystemTime>` y `is_expired` compara con el reloj en cada `GET` (`src/storage.rs:18-21`). Evita un hilo o temporizador de limpieza, pero deja memoria ocupada por claves vencidas y hace que el coste de comprobación recaiga en cada lectura.

## Estructura del proyecto

```text
Cargo.toml        # manifiesto del crate: edición 2024, tokio, bytes, anyhow
src/
├── main.rs       # listener TCP, tarea por conexión, bucle lectura-parseo-respuesta
├── commands.rs   # dispatch de PING, ECHO, SET y GET; validación y expiración
├── parser.rs     # parser incremental de mensajes RESP2
├── resp.rs       # enum RespValue y serialización a RESP2
└── storage.rs    # alias Storage, Entry y comprobación de expiración
```

## Próximos pasos

- [ ] Añadir `DEL`, `EXISTS` e `INCR`, ausentes del dispatch de `src/commands.rs`.
- [ ] Implementar expiración activa (barrido periódico) para liberar claves vencidas.
- [ ] Hacer configurable la dirección y el puerto, hoy fijos en `src/main.rs:18`.
- [ ] Añadir tests de integración sobre TCP: varios clientes, pipelining y cierres abruptos.
