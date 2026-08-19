import asyncio
import json


HOST = "127.0.0.1"
PORT = 5000


async def send_event(writer, event, data):

    message = {
        "event": event,
        "data": data
    }

    raw = json.dumps(
        message,
        ensure_ascii=False
    ) + "\n"

    writer.write(
        raw.encode("utf-8")
    )

    await writer.drain()


async def receive_events(reader):

    while True:

        line = await reader.readline()

        if not line:
            print("\n[SERVER] Connection closed")
            break

        try:

            response = json.loads(
                line.decode("utf-8")
            )

            event = response.get("event")
            data = response.get("data", {})

            if event == "chat":

                print(
                    f"\n[{data['room']}] "
                    f"{data['from']}: "
                    f"{data['message']}"
                )

            elif event == "private_chat":

                print(
                    f"\n[PM] "
                    f"{data['from']}: "
                    f"{data['message']}"
                )

            elif event == "broadcast":

                print(
                    f"\n[BROADCAST] "
                    f"{data['from']}: "
                    f"{data['message']}"
                )

            elif event == "user_joined":

                print(
                    f"\n[ROOM] "
                    f"{data['username']} joined "
                    f"{data['room']}"
                )

            elif event == "user_left":

                print(
                    f"\n[ROOM] "
                    f"{data['username']} left "
                    f"{data['room']}"
                )

            elif event == "user_disconnected":

                print(
                    f"\n[ROOM] "
                    f"{data['username']} disconnected"
                )

            elif event == "users":

                print("\n[ONLINE USERS]")

                for username in data["users"]:
                    print(f"- {username}")

            elif event == "rooms":

                print("\n[ROOMS]")

                for room, members in data["rooms"].items():

                    print(
                        f"- {room}: "
                        f"{', '.join(members)}"
                    )

            elif event == "error":

                print(
                    f"\n[ERROR] "
                    f"{data['message']}"
                )

            elif event == "create_user_success":

                print(
                    f"\n[CONNECTED] "
                    f"Username: {data['username']}"
                )

            elif event == "join_room_success":

                print(
                    f"\n[JOINED] "
                    f"{data['room']}"
                )

            elif event == "leave_room_success":

                print(
                    f"\n[LEFT] "
                    f"{data['room']}"
                )

        except json.JSONDecodeError:

            print("[ERROR] Invalid JSON")


def print_help():

    print("""
========================================
Commands
========================================

/join <room>
    Join a room

/leave <room>
    Leave a room

/msg <room> <message>
    Send message to room

/pm <user> <message>
    Private message

/broadcast <message>
    Broadcast to everyone

/users
    Show online users

/rooms
    Show rooms

/quit
    Disconnect

/help
    Show this help

========================================
""")


async def main():

    reader, writer = await asyncio.open_connection(
        HOST,
        PORT
    )

    print("Connected to server")

    username = input("Username: ")

    await send_event(
        writer,
        "create_user",
        {
            "username": username
        }
    )

    response = await reader.readline()

    response = json.loads(
        response.decode("utf-8")
    )

    if response["event"] == "error":

        print(
            f"Error: "
            f"{response['data']['message']}"
        )

        writer.close()
        await writer.wait_closed()

        return

    print(
        f"Logged in as "
        f"{username}"
    )

    print_help()

    receive_task = asyncio.create_task(
        receive_events(reader)
    )

    try:

        while True:

            command = await asyncio.to_thread(
                input,
                "> "
            )

            if not command:
                continue

            parts = command.split(" ", 2)

            cmd = parts[0]

            if cmd == "/join":

                if len(parts) < 2:
                    print("Usage: /join <room>")
                    continue

                await send_event(
                    writer,
                    "join_room",
                    {
                        "room": parts[1]
                    }
                )

            elif cmd == "/leave":

                if len(parts) < 2:
                    print("Usage: /leave <room>")
                    continue

                await send_event(
                    writer,
                    "leave_room",
                    {
                        "room": parts[1]
                    }
                )

            elif cmd == "/msg":

                if len(parts) < 3:
                    print(
                        "Usage: "
                        "/msg <room> <message>"
                    )

                    continue

                await send_event(
                    writer,
                    "chat",
                    {
                        "room": parts[1],
                        "message": parts[2]
                    }
                )

            elif cmd == "/pm":

                if len(parts) < 3:
                    print(
                        "Usage: "
                        "/pm <user> <message>"
                    )

                    continue

                await send_event(
                    writer,
                    "private_chat",
                    {
                        "to": parts[1],
                        "message": parts[2]
                    }
                )

            elif cmd == "/broadcast":

                if len(parts) < 2:
                    print(
                        "Usage: "
                        "/broadcast <message>"
                    )

                    continue

                await send_event(
                    writer,
                    "broadcast",
                    {
                        "message": parts[1]
                    }
                )

            elif cmd == "/users":

                await send_event(
                    writer,
                    "list_users",
                    {}
                )

            elif cmd == "/rooms":

                await send_event(
                    writer,
                    "list_rooms",
                    {}
                )

            elif cmd == "/help":

                print_help()

            elif cmd == "/quit":

                await send_event(
                    writer,
                    "disconnect",
                    {}
                )

                break

            else:

                print(
                    "Unknown command. "
                    "Use /help"
                )

    finally:

        receive_task.cancel()

        writer.close()

        await writer.wait_closed()

        print("Disconnected")


if __name__ == "__main__":
    asyncio.run(main())