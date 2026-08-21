import asyncio
import json

HOST = "0.0.0.0"
PORT = 5000

# Holds every connected user: {username: writer}
# This is how the server knows who is online and where to send messages.
users = {}


# ---------------------------------------------------------
# Sends one JSON event to a single client (one line + "\n")
# ---------------------------------------------------------
async def send_event(writer, event, data):
    message = {"event": event, "data": data}
    raw = json.dumps(message, ensure_ascii=False) + "\n"

    writer.write(raw.encode("utf-8"))
    await writer.drain()


# ---------------------------------------------------------
# Sends one JSON event to EVERY connected user (except "exclude")
# This is what makes messages visible to everyone.
# ---------------------------------------------------------
async def broadcast(event, data, exclude=None):
    for username, writer in list(users.items()):
        if username == exclude:
            continue
        try:
            await send_event(writer, event, data)
        except (ConnectionResetError, BrokenPipeError):
            pass  # user disconnected mid-send, ignore


# ---------------------------------------------------------
# Registers a new user (first thing every client must do)
# ---------------------------------------------------------
async def create_user(writer, username):
    if not username or username in users:
        await send_event(writer, "error", {"message": "Username invalid or already taken"})
        return False

    users[username] = writer

    # Confirm login to the new user (client waits for this reply)
    await send_event(writer, "create_user_success", {"username": username})

    # Let everyone else know a new user joined
    await broadcast("user_joined", {"username": username}, exclude=username)

    print(f"[CONNECT] {username}")
    return True


# ---------------------------------------------------------
# Removes a user when they disconnect / send "disconnect"
# ---------------------------------------------------------
async def disconnect_user(username):
    if username not in users:
        return

    writer = users.pop(username)

    try:
        writer.close()
        await writer.wait_closed()
    except Exception:
        pass

    await broadcast("user_disconnected", {"username": username})
    print(f"[DISCONNECT] {username}")


# ---------------------------------------------------------
# Handles one client connection for its whole lifetime:
#   1. First message must be create_user
#   2. Then it only accepts "message" (send to everyone) and "disconnect"
# ---------------------------------------------------------
async def handle_client(reader, writer):
    username = None

    try:
        # --- Step 1: login ---
        first_line = await reader.readline()
        if not first_line:
            return

        request = json.loads(first_line.decode("utf-8"))

        if request.get("event") != "create_user":
            await send_event(writer, "error", {"message": "First event must be create_user"})
            return

        username = request["data"]["username"]

        if not await create_user(writer, username):
            username = None  # login failed, nothing to clean up
            return

        # --- Step 2: chat loop ---
        while True:
            line = await reader.readline()
            if not line:
                break  # client closed the connection

            request = json.loads(line.decode("utf-8"))
            event = request.get("event")
            data = request.get("data", {})

            if event == "message":
                # A user sent a message -> everyone (including sender) sees who sent it
                text = data.get("message", "")
                await broadcast("message", {"from": username, "message": text})

            elif event == "disconnect":
                break

            else:
                await send_event(writer, "error", {"message": f"Unknown event: {event}"})

    except json.JSONDecodeError:
        await send_event(writer, "error", {"message": "Invalid JSON"})

    except (ConnectionResetError, BrokenPipeError):
        pass

    finally:
        if username is not None:
            await disconnect_user(username)


# ---------------------------------------------------------
# Starts the TCP server
# ---------------------------------------------------------
async def main():
    server = await asyncio.start_server(handle_client, HOST, PORT)
    addr = ", ".join(str(sock.getsockname()) for sock in server.sockets)

    print("================================")
    print(" TCP Chat Server")
    print("================================")
    print(f"Listening on: {addr}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())