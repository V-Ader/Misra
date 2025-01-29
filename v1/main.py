import asyncio
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class StoredToken:
    NONE = 0
    PING = 1
    PONG = 2
    BOTH = 3


class TokenType:
    PING_TOKEN = 0
    PONG_TOKEN = 1


class MisraSocket:
    def __init__(self, init):
        self.initializer = init
        self.send_writer = None
        self.listen_reader = None
        self.listen_writer = None
        self.m = 0
        self.ping = 1
        self.pong = -1
        self.stored_token = StoredToken.NONE
        self.read_channel = asyncio.Queue()

    async def send(self, token_type):
        if not self.send_writer:
            logging.error("Send connection is not established.")
            return

        if token_type == TokenType.PING_TOKEN:
            token = self.ping
            if self.stored_token == StoredToken.PING:
                self.stored_token = StoredToken.NONE
            elif self.stored_token == StoredToken.BOTH:
                self.stored_token = StoredToken.PONG
        elif token_type == TokenType.PONG_TOKEN:
            token = self.pong
            if self.stored_token == StoredToken.PONG:
                self.stored_token = StoredToken.NONE
            elif self.stored_token == StoredToken.BOTH:
                self.stored_token = StoredToken.PING
        else:
            return

        self.m = token
        self.send_writer.write(f"{token}\n".encode())
        await self.send_writer.drain()
        logging.info("Token sent: %s", "PING" if token_type == TokenType.PING_TOKEN else "PONG")

    async def init_outgoing_connection(self, send_address, send_port):
        logging.info("Trying to connect to %s:%d", send_address, send_port)
        while True:
            try:
                self.send_reader, self.send_writer = await asyncio.open_connection(send_address, send_port)
                logging.info("Successfully connected to %s:%d", send_address, send_port)
                if self.initializer:
                    self.send_init_message()
                break
            except ConnectionError as e:
                logging.error("Error connecting to %s:%d: %s. Retrying...", send_address, send_port, e)
                await asyncio.sleep(1)

    def send_init_message(self):
        logging.info("sent init")

        self.stored_token = StoredToken.BOTH
        asyncio.create_task(self.send(TokenType.PING_TOKEN))
        asyncio.create_task(self.send(TokenType.PONG_TOKEN))

    async def listen(self, listen_port):
        server = await asyncio.start_server(self.handle_client, host="0.0.0.0", port=listen_port)
        logging.info("Listening on port %d", listen_port)
        async with server:
            await server.serve_forever()

    async def handle_client(self, reader, writer):
        self.listen_reader = reader
        self.listen_writer = writer
        logging.info("Connection established with %s", writer.get_extra_info("peername"))
        asyncio.create_task(self.read_from_connection())

    async def handle_messages(self):
        while True:
            if self.stored_token == StoredToken.NONE:
                token = await self.read_channel.get()
                self.receive_token(token)
            elif self.stored_token == StoredToken.PING:
                logging.info("Ping token acquired, entering critical section")
                await asyncio.sleep(1)
                logging.info("Leaving critical section")
                token = await self.read_channel.get()
                self.receive_token(token)
                continue
            elif self.stored_token == StoredToken.PONG:
                await self.send(TokenType.PONG_TOKEN)
            elif self.stored_token == StoredToken.BOTH:
                logging.info("Both tokens acquired, incarnating")
                self.incarnate()
                await self.send(TokenType.PING_TOKEN)
                await self.send(TokenType.PONG_TOKEN)

    def receive_token(self, token):
        if abs(token) < abs(self.m):
            logging.info("Old token, ignoring")
            return

        if token == self.m:
            if self.m > 0:
                logging.warning("Pong token lost, regenerating")
                self.regenerate_tokens()
                return
            elif self.m < 0:
                logging.warning("Ping token lost, regenerating")
                self.regenerate_tokens()
                return

        if token > 0:
            self.ping = token
            self.pong = -self.ping
            if self.stored_token == StoredToken.NONE:
                self.stored_token = StoredToken.PING
            elif self.stored_token == StoredToken.PONG:
                self.stored_token = StoredToken.BOTH
            else:
                logging.error("Something went very wrong!")
        elif token < 0:
            self.pong = token
            self.ping = abs(token)
            if self.stored_token == StoredToken.NONE:
                self.stored_token = StoredToken.PONG
            elif self.stored_token == StoredToken.PING:
                self.stored_token = StoredToken.BOTH
            else:
                logging.error("Something went very wrong!")

    def regenerate_tokens(self):
        self.stored_token = StoredToken.BOTH

    def incarnate(self):
        self.ping += 1
        self.pong = -self.ping

    async def read_from_connection(self):
        while True:
            line = await self.listen_reader.readline()
            if not line:
                break
            token = int(line.decode().strip())
            await self.read_channel.put(token)


async def main():
    listen_port = int(sys.argv[1])
    send_address = sys.argv[2]
    send_port = int(sys.argv[3])

    init = int(sys.argv[4])
    socket = MisraSocket(init)




    if init == True:
        logging.info("init")
        await asyncio.gather(
            socket.init_outgoing_connection(send_address, send_port),
            socket.listen(listen_port),
            socket.handle_messages(),
        )

    else:
        await asyncio.gather(
            socket.init_outgoing_connection(send_address, send_port),
            socket.listen(listen_port),
            socket.handle_messages()
        )


if __name__ == "__main__":
    asyncio.run(main())
