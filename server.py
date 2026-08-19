import asyncio
import json


HOST = "0.0.0.0"
PORT = 5000


# username -> writer
users = {}

# room_name -> set(username)
rooms = {}


async def send_event(writer, event, data):
    message = {
        "event": event,
        "data": data
    }

    raw = json.dumps(message, ensure_ascii=False) + "\n"

    writer.write(raw.encode("utf-8"))

    await writer.drain()


async def broadcast(room_name, event, data, exclude=None):
    if room_name not in rooms:
        return

    message = {
        "event": event,
        "data": data
    }

    raw = json.dumps(message, ensure_ascii=False) + "\n"
    encoded = raw.encode("utf-8")

    for username in rooms[room_name].copy():

        if username == exclude:
            continue

        writer = users.get(username)

        if writer is None:
            continue

        try:
            writer.write(encoded)
            await writer.drain()

        except (ConnectionResetError, BrokenPipeError):
            pass


async def create_user(writer, username):

    if username in users:
        await send_event(
            writer,
            "error",
            {
                "message": "Username already exists"
            }
        )

        return False

    users[username] = writer

    await send_event(
        writer,
        "create_user_success",
        {
            "username": username
        }
    )

    print(f"[CONNECT] {username}")

    return True


async def join_room(username, room_name):

    if room_name not in rooms:
        rooms[room_name] = set()

    if username in rooms[room_name]:
        return

    rooms[room_name].add(username)

    await send_event(
        users[username],
        "join_room_success",
        {
            "room": room_name
        }
    )

    await broadcast(
        room_name,
        "user_joined",
        {
            "username": username,
            "room": room_name
        },
        exclude=username
    )

    print(f"[ROOM] {username} joined {room_name}")


async def leave_room(username, room_name):

    if room_name not in rooms:
        return

    if username not in rooms[room_name]:
        return

    rooms[room_name].remove(username)

    await send_event(
        users[username],
        "leave_room_success",
        {
            "room": room_name
        }
    )

    await broadcast(
        room_name,
        "user_left",
        {
            "username": username,
            "room": room_name
        },
        exclude=username
    )

    if len(rooms[room_name]) == 0:
        del rooms[room_name]


async def chat(username, room_name, message):

    if room_name not in rooms:
        await send_event(
            users[username],
            "error",
            {
                "message": "Room does not exist"
            }
        )

        return

    if username not in rooms[room_name]:
        await send_event(
            users[username],
            "error",
            {
                "message": "You are not in this room"
            }
        )

        return

    await broadcast(
        room_name,
        "chat",
        {
            "from": username,
            "room": room_name,
            "message": message
        }
    )


async def private_chat(username, target, message):

    writer = users.get(target)

    if writer is None:
        await send_event(
            users[username],
            "error",
            {
                "message": "User not found"
            }
        )

        return

    await send_event(
        writer,
        "private_chat",
        {
            "from": username,
            "message": message
        }
    )


async def broadcast_all(username, message):

    for target_username, writer in users.items():

        try:
            await send_event(
                writer,
                "broadcast",
                {
                    "from": username,
                    "message": message
                }
            )

        except (ConnectionResetError, BrokenPipeError):
            pass


async def list_users(username):

    await send_event(
        users[username],
        "users",
        {
            "users": list(users.keys())
        }
    )


async def list_rooms(username):

    room_data = {}

    for room_name, members in rooms.items():
        room_data[room_name] = list(members)

    await send_event(
        users[username],
        "rooms",
        {
            "rooms": room_data
        }
    )


async def disconnect_user(username):

    if username not in users:
        return

    # Remove user from every room
    for room_name in list(rooms.keys()):

        if username in rooms[room_name]:

            rooms[room_name].remove(username)

            await broadcast(
                room_name,
                "user_disconnected",
                {
                    "username": username,
                    "room": room_name
                },
                exclude=username
            )

            if len(rooms[room_name]) == 0:
                del rooms[room_name]

    writer = users.pop(username)

    try:
        writer.close()
        await writer.wait_closed()

    except Exception:
        pass

    print(f"[DISCONNECT] {username}")


async def handle_client(reader, writer):

    username = None

    try:

        first_line = await reader.readline()

        if not first_line:
            return

        request = json.loads(
            first_line.decode("utf-8")
        )

        if request.get("event") != "create_user":
            await send_event(
                writer,
                "error",
                {
                    "message": "First event must be create_user"
                }
            )

            return

        username = request["data"]["username"]

        success = await create_user(
            writer,
            username
        )

        if not success:
            return

        while True:

            line = await reader.readline()

            if not line:
                break

            request = json.loads(
                line.decode("utf-8")
            )

            event = request.get("event")
            data = request.get("data", {})

            if event == "join_room":

                await join_room(
                    username,
                    data["room"]
                )

            elif event == "leave_room":

                await leave_room(
                    username,
                    data["room"]
                )

            elif event == "chat":

                await chat(
                    username,
                    data["room"],
                    data["message"]
                )

            elif event == "private_chat":

                await private_chat(
                    username,
                    data["to"],
                    data["message"]
                )

            elif event == "broadcast":

                await broadcast_all(
                    username,
                    data["message"]
                )

            elif event == "list_users":

                await list_users(username)

            elif event == "list_rooms":

                await list_rooms(username)

            elif event == "disconnect":

                break

            else:

                await send_event(
                    writer,
                    "error",
                    {
                        "message": f"Unknown event: {event}"
                    }
                )

    except json.JSONDecodeError:

        await send_event(
            writer,
            "error",
            {
                "message": "Invalid JSON"
            }
        )

    except (ConnectionResetError, BrokenPipeError):

        pass

    finally:

        if username is not None:
            await disconnect_user(username)


async def main():

    server = await asyncio.start_server(
        handle_client,
        HOST,
        PORT
    )

    addresses = ", ".join(
        str(sock.getsockname())
        for sock in server.sockets
    )

    print("================================")
    print(" Asyncio TCP Chat Server")
    print("================================")
    print(f"Server running at: {addresses}")
    print("Waiting for clients...")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())