const express = require("express");
const http = require("http");
const socketio = require("socket.io");
const app = express();
const server = http.createServer(app);
const io = socketio(server);

const users = [];

function userJoin(id, username, room) {
  const user = { id, username, room };

  users.push(user);

  return user;
}

function getCurrentUser(id) {
  return users.find((user) => user.id === id);
}

function userLeave(id) {
  const index = users.findIndex((user) => user.id === id);

  if (index !== -1) {
    return users.splice(index, 1)[0];
  }
}

function getRoomUsers(room) {
  return users.filter((user) => user.room === room);
}

app.get("/", (req, res) => {
  res.sendFile(__dirname + "/public/index.html");
});

io.on("connection", (socket) => {
  socket.on("joinRoom", ({ username, room }) => {
    const user = userJoin(socket.id, username, room);

    socket.join(user.room);

    // socket.emit('message', `${username} join the room`)

    socket.broadcast.to(user.room).emit("message", `${username} join the room`);
  });

  socket.on("chatMessage", (msg) => {
    const user = getCurrentUser(socket.id);

    if (msg === "exit") {
      socket.emit("message", "you leave the room");
      socket.broadcast.to(user.room).emit("message", `${username} has left`);
    }

    io.to(user.room).emit("message", `${username}:${msg}`);
  });

  socket.on("disconnect", () => {
    const user = userLeave(socket.id);

    if (user) {
      io.to(user.room).emit(
        "message",
        `${username} disconnected`
      );
    }
  });
});

server.listen(3000, () => {
  console.log("listening on localhost:3000");
});
