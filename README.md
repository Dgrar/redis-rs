# codecrafters-redis

Servidor TCP que implementa un subconjunto de Redis sobre RESP2: strings con expiración, listas y `BLPOP` bloqueante sobre Tokio. Reto «Build your own Redis» de CodeCrafters.

## Estado y funcionalidades actuales

Prototipo WIP. Compila con Rust edición 2024 y pasa los 14 tests unitarios existentes (`cargo test`): parser RESP2 y serialización. Los handlers de comandos no tienen tests.

- **`PING` / `ECHO`**: `+PONG` y eco exacto de un argumento.
- **`SET` / `GET`**: `SET key value [PX ms | EX s]` con validación de expiración; `GET` devuelve bulk string o `$-1` si la clave no existe o expiró (expiración perezosa).
- **`RPUSH` / `LPUSH`**: crean o modifican una lista y devuelven su longitud; `LPUSH` respeta el orden de Redis (el último argumento queda primero en la cabeza).
- **`LRANGE` / `LLEN`**: rango inclusivo con índices negativos (`0 -1` = lista completa); `LLEN` devuelve `0` si la clave no existe.
- **`LPOP`**: `LPOP key [count]`; devuelve bulk string o array, y elimina la clave cuando la lista queda vacía.
- **`BLPOP`**: `BLPOP key [key...] timeout`; prueba las claves en orden, espera con `tokio::sync::Notify` sin bloquear el runtime, acepta timeout decimal en segundos (`0` = infinito) y devuelve array nulo (`*-1`) al vencer.
- **Parser RESP2 incremental** y **pipelining**: varios mensajes por lectura y mensajes partidos entre paquetes TCP.
- **Concurrencia**: una tarea Tokio por conexión, `HashMap` global bajo `RwLock` y un `Notify` global para despertar a los `BLPOP`.

Limitaciones conocidas:
- Solo parser y serialización tienen tests; `SET`, `GET`, listas y `BLPOP` no tienen cobertura automatizada.
- `GET`, `LRANGE` y `LLEN` no comprueban el tipo: `GET` de una lista devuelve su codificación interna (`0x02...`) y `LRANGE`/`LLEN` de un string interpretan bytes arbitrarios como lista. `SET` sobrescribe listas sin `WRONGTYPE`.
- Expiración perezosa solo en `GET` y sin barrido; no hay `DEL`/`EXISTS`, replicación ni persistencia; la dirección está fija en `127.0.0.1:6379`; quedan `println!` de depuración en `LPUSH` y `LRANGE`.

## Requisitos y ejecución rápida

- Rust 1.85+ (edición 2024). `codecrafters.yml` fija `rust-1.96`; verificado con `rustc 1.98.1` en Windows.
- Sin servicios externos: el servidor escucha en `127.0.0.1:6379` (fijo; no lee flags ni variables de entorno).
- Cliente RESP opcional para pruebas manuales (`redis-cli` u otro).

```bash
# compilar
cargo build --release

# tests unitarios
cargo test

# arrancar (bloquea la terminal)
cargo run --release
```

En Unix, `./your_program.sh` compila a `/tmp/codecrafters-build-redis-rust` y arranca el binario.

## Ejemplo de uso real

Con el servidor en marcha en otra terminal:

```bash
redis-cli -p 6379 PING
redis-cli -p 6379 RPUSH lista a b c
redis-cli -p 6379 LRANGE lista 0 -1
redis-cli -p 6379 BLPOP lista 0
redis-cli -p 6379 BLPOP vacia 0.1
redis-cli -p 6379 SET temporal hola PX 50
sleep 0.1
redis-cli -p 6379 GET temporal
```

Salida esperada (derivada de `src/commands.rs` y `src/resp.rs`; la secuencia RESP se contrastó con una ejecución real por socket, sin `redis-cli` en el entorno):

```text
PONG
(integer) 3
1) "a"
2) "b"
3) "c"
1) "lista"
2) "a"
(nil)
OK
(nil)
```

`BLPOP vacia 0.1` vence a los 100 ms y responde array nulo; `GET temporal` ya no encuentra la clave por el `PX 50`.

## Decisiones de arquitectura y aprendizaje

- **Listas en un único `Vec<u8>`** (`src/commands.rs:161`, `src/commands.rs:177`): el `Entry.value` de una lista es `[0x02][longitud u32 BE][bytes]...`. Evita una asignación por elemento y reutiliza el mismo `Entry` que los strings; el precio es O(n): `LPUSH` reconstruye el buffer y `LPOP`/`BLPOP` desplazan el resto con `copy_within`.
- **Estado compartido** (`src/storage.rs:5`, `src/main.rs:22`): `Arc<RwLock<HashMap<Vec<u8>, Entry>>>` global. `GET`, `LRANGE` y `LLEN` toman el lock de lectura (concurrentes) y las mutaciones el de escritura; un lock global serializa escrituras y bloquea lecturas mientras dura una escritura.
- **BLPOP sin bloquear el runtime** (`src/commands.rs:363`): patrón comprobar → crear `notified()` + `enable()` → recomprobar → `timeout`/`await`. Registrar el waiter antes de la segunda comprobación evita el *lost wakeup*; ningún `RwLock` cruza un `await`. `pop_left` (`src/commands.rs:419`) clona el elemento para soltar el lock antes de esperar. Compromiso: `notify_waiters()` despierta a todos los clientes y no hay FIFO estricto.
- **Dispatch async selectivo** (`src/commands.rs:10`, `src/main.rs:60`): `execute_command` es `async` para permitir la espera de `BLPOP`; mientras una conexión espera, el runtime sigue atendiendo al resto. La conexión bloqueada deja de leer, igual que Redis.
- **Errores y parsing** (`src/parser.rs:3`, `src/main.rs:56`): parser incremental que devuelve `Option<(RespValue, usize)>`. Un error de protocolo responde error RESP y cierra la conexión; un error de comando viaja como `RespValue::Error` sin cerrarla. No hay enum de errores tipado: el parser usa `Result<_, String>` y `anyhow` queda para I/O en `main`.
- **Nulo frente a vacío en RESP2** (`src/resp.rs:16`): `BulkString(None)` → `$-1`, `Array(None)` → `*-1`, `Array(Some(vec![]))` → `*0`. La distinción es deliberada: el timeout de `BLPOP` y `LPOP key count` sobre clave ausente son nulos, mientras `LRANGE` de una clave inexistente es un array vacío.

## Estructura del proyecto

```text
Cargo.toml        # manifiesto: edición 2024; tokio, bytes, anyhow
src/
├── main.rs       # listener TCP, tarea por conexión, bucle lectura-parseo-respuesta, Notify global
├── commands.rs   # dispatch async: strings, listas, BLPOP y codificación interna de listas
├── parser.rs     # parser incremental de mensajes RESP2
├── resp.rs       # enum RespValue y serialización RESP2
└── storage.rs    # alias Storage, Entry y comprobación de expiración
```

## Próximos pasos

- [ ] Restaurar tests de los handlers (`SET`, `GET`, listas y `BLPOP`); hoy solo hay 14 tests de parser y serialización.
- [ ] Validar tipo de clave en `GET`, `LRANGE`, `LLEN` y `SET` (`WRONGTYPE`), como ya hacen `RPUSH`/`LPUSH`/`LPOP`.
- [ ] Sustituir la expiración perezosa por un barrido periódico o purga en escritura.
- [ ] Añadir tests de integración TCP con varios clientes y varios `BLPOP` esperando la misma clave.
