import asyncio
import json
import sys

HOST = "127.0.0.1"
PORT = 5000


# ---------------------------------------------------------
# Sends one JSON event to the server (one line + "\n")
# ---------------------------------------------------------
async def send_event(writer, event, data):
    message = {"event": event, "data": data}
    raw = json.dumps(message, ensure_ascii=False) + "\n"

    writer.write(raw.encode("utf-8"))
    await writer.drain()


# ---------------------------------------------------------
# Prints a line without leaving a leftover blank "user > "
# prompt above it. "\r" moves to start of line and "\033[K"
# clears it, wiping out the empty prompt before we print,
# then the prompt is redrawn so the user can keep typing.
# ---------------------------------------------------------
current_prompt = ""

def print_clean(text):
    sys.stdout.write("\r\033[K" + text + "\n" + current_prompt)
    sys.stdout.flush()


# ---------------------------------------------------------
# Background task: continuously listens for events from the
# server and prints them. This is how incoming messages
# (from anyone, including replies) show up on screen.
# ---------------------------------------------------------
async def receive_events(reader):
    while True:
        line = await reader.readline()

        if not line:
            print_clean("[SERVER] Connection closed")
            break

        try:
            response = json.loads(line.decode("utf-8"))
            event = response.get("event")
            data = response.get("data", {})

            if event == "message":
                # Shows who sent the message -> works for both new
                # messages and replies, since a reply is just another message.
                print_clean(f"{data['from']}: {data['message']}")

            elif event == "user_joined":
                print_clean(f"[ROOM] {data['username']} joined the chat")

            elif event == "user_disconnected":
                print_clean(f"[ROOM] {data['username']} disconnected")

            elif event == "error":
                print_clean(f"[ERROR] {data['message']}")

            elif event == "create_user_success":
                print_clean(f"[CONNECTED] Username: {data['username']}")

        except json.JSONDecodeError:
            print_clean("[ERROR] Invalid JSON received")


# ---------------------------------------------------------
# Main flow: connect, log in, then loop reading user input
# and sending it as a "message" event to the server.
# ---------------------------------------------------------
async def main():
    reader, writer = await asyncio.open_connection(HOST, PORT)
    print("Connected to server")

    # --- login ---
    username = input("Username: ")
    if len(username) == 0:
        username = "Anonymous"
    await send_event(writer, "create_user", {"username": username})

    response = json.loads((await reader.readline()).decode("utf-8"))

    if response["event"] == "error":
        print(f"Error: {response['data']['message']}")
        writer.close()
        await writer.wait_closed()
        return

    print(f"Logged in as {username}")

    # Start listening for messages in the background so we can
    # receive and send at the same time.
    receive_task = asyncio.create_task(receive_events(reader))

    global current_prompt
    current_prompt = f"{username} > "

    try:
        while True:
            text = await asyncio.to_thread(input, current_prompt)

            if not text:
                continue

            if text == "/quit":
                await send_event(writer, "disconnect", {})
                break

            # Anything typed is just sent as a message.
            # Replying works the same way -> type and send.
            await send_event(writer, "message", {"message": text})

    finally:
        receive_task.cancel()
        writer.close()
        await writer.wait_closed()
        print("Disconnected")


if __name__ == "__main__":
    asyncio.run(main())