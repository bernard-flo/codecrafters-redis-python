import asyncio

from app.expiring_dict import ExpiringDict


async def handle_command(command_line: list[str], writer: asyncio.StreamWriter) -> None:
    await write_simple_string(writer, "")


async def handle_ping(command_line: list[str], writer: asyncio.StreamWriter) -> None:
    await write_simple_string(writer, "PONG")


async def handle_echo(command_line: list[str], writer: asyncio.StreamWriter) -> None:
    arg = command_line[1]
    await write_simple_string(writer, arg)


async def handle_get(command_line: list[str], writer: asyncio.StreamWriter) -> None:
    key = command_line[1]
    value = expiring_dict.get(key)
    if value:
        await write_simple_string(writer, value)
    else:
        await write_null_string(writer)


async def handle_set(command_line: list[str], writer: asyncio.StreamWriter) -> None:
    key = command_line[1]
    value = command_line[2]
    expiry_ms = None
    if len(command_line) >= 5:
        expiry_type = command_line[3]
        assert expiry_type == "px"
        expiry_ms = int(command_line[4])
    expiring_dict.put(key, value, expiry_ms)
    await write_simple_string(writer, "OK")


command_line_handler_map = {
    "command": handle_command,
    "ping": handle_ping,
    "echo": handle_echo,
    "get": handle_get,
    "set": handle_set,
}

expiring_dict = ExpiringDict[str, str]()

NEW_LINE_BYTES = "\r\n".encode()


async def write_line(writer: asyncio.StreamWriter, line: str) -> None:
    writer.write(line.encode())
    writer.write(NEW_LINE_BYTES)
    await writer.drain()


async def write_simple_string(writer: asyncio.StreamWriter, simple_string: str) -> None:
    line = "+" + simple_string
    await write_line(writer, line)


async def write_null_string(writer: asyncio.StreamWriter) -> None:
    line = "$-1"
    await write_line(writer, line)


async def read_line(reader: asyncio.StreamReader) -> str:
    return (await reader.readline())[:-2].decode()


async def read_bulk_string(reader: asyncio.StreamReader) -> str:
    await read_line(reader)
    return await read_line(reader)


async def read_array(reader: asyncio.StreamReader) -> list[str]:
    array: list[str] = list()

    line = await read_line(reader)
    if not line:
        return array

    assert line.startswith("*")

    size = int(line[1:])
    for i in range(size):
        bulk_string = await read_bulk_string(reader)
        array.append(bulk_string)

    return array


async def read_command_line(reader: asyncio.StreamReader) -> list[str]:
    return await read_array(reader)


async def handle_command_line(command_line: list[str], writer: asyncio.StreamWriter) -> None:
    command = command_line[0].lower()
    await command_line_handler_map[command](command_line, writer)


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    while not reader.at_eof():
        command_line = await read_command_line(reader)
        if not command_line:
            break

        await handle_command_line(command_line, writer)


async def main() -> None:
    server = await asyncio.start_server(handle_client, port=6379)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
