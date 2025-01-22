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
    def __init__(self):
        self.initialization()

        self.send_writer = None
        self.listen_reader = None
        self.listen_writer = None
        self.m = 0
        self.stored_token = StoredToken.NONE
        self.read_channel = asyncio.Queue()


    def initialization(self):
        self.ping = 1
        self.pong = -1

    async def init_outgoing_connection(self, send_address, send_port, init):
        logging.info("Attempting to connect to %s:%d", send_address, send_port)
        while not self.send_writer:
            try:
                self.send_reader, self.send_writer = await asyncio.open_connection(send_address, send_port)
                logging.info("Connected to %s:%d", send_address, send_port)
                if init:
                    await self.send_initial_tokens()
            except ConnectionError as e:
                logging.error("Connection failed: %s. Retrying...", e)
                await asyncio.sleep(1)


    async def send_initial_tokens(self):
        logging.info("Sending initial tokens")
        self.stored_token = StoredToken.BOTH
        await asyncio.gather(
            self.send(TokenType.PING_TOKEN),
            self.send(TokenType.PONG_TOKEN)
        )

    async def listen(self, listen_port):
        server = await asyncio.start_server(self.handle_client, host="0.0.0.0", port=listen_port)
        logging.info("Listening on port %d", listen_port)
        async with server:
            await server.serve_forever()

    async def handle_client(self, reader, writer):
        self.listen_reader = reader
        self.listen_writer = writer
        peername = writer.get_extra_info("peername")
        logging.info("Connection established with %s", peername)
        asyncio.create_task(self.read_from_connection())

    async def read_from_connection(self):
        while True:
            try:
                line = await self.listen_reader.readline()
                if not line:
                    logging.warning("Connection closed by peer")
                    break
                token = int(line.decode().strip())
                await self.read_channel.put(token)
                logging.debug("Received token: %d", token)
            except ValueError:
                logging.error("Failed to parse token. Ignoring invalid input.")

    async def send(self, token_type):
        if not self.send_writer:
            logging.error("Send connection is not established.")
            return

        token = self.ping if token_type == TokenType.PING_TOKEN else self.pong
        if token_type == TokenType.PING_TOKEN:
            self.stored_token = (StoredToken.NONE if self.stored_token == StoredToken.PING else
                                 StoredToken.PONG if self.stored_token == StoredToken.BOTH else
                                 self.stored_token)
        elif token_type == TokenType.PONG_TOKEN:
            self.stored_token = (StoredToken.NONE if self.stored_token == StoredToken.PONG else
                                 StoredToken.PING if self.stored_token == StoredToken.BOTH else
                                 self.stored_token)

        self.m = token
        self.send_writer.write(f"{token}\n".encode())
        await self.send_writer.drain()
        logging.info("Sent token: %s", "PING" if token_type == TokenType.PING_TOKEN else "PONG")

    async def handle_messages(self):
        while True:
            token = await self.read_channel.get()
            self.process_token(token)
            
            if self.stored_token == StoredToken.PING:
                logging.info("PING token acquired, entering critical section")
                await asyncio.sleep(1)
                logging.info("Exiting critical section")

            elif self.stored_token == StoredToken.BOTH:
                logging.info("Both tokens acquired, performing incarnation")
                self.incarnate()
                await asyncio.gather(
                    self.send(TokenType.PING_TOKEN),
                    self.send(TokenType.PONG_TOKEN)
                )

    def process_token(self, token):
        if abs(token) < abs(self.m):
            logging.info("Received old token. Ignoring.")
            return

        if token == self.m:
            logging.warning("Duplicate token detected. Regenerating.")
            self.regenerate_tokens()
            return

        if token > 0:
            self.update_tokens(new_ping=token, is_ping=True)
        elif token < 0:
            self.update_tokens(new_ping=-token, is_ping=False)

    def update_tokens(self, new_ping, is_ping):
        self.ping = new_ping
        self.pong = -new_ping
        self.stored_token = (StoredToken.PING if is_ping else StoredToken.PONG
                              if self.stored_token == StoredToken.NONE else
                              StoredToken.BOTH)
        logging.info("Updated tokens: PING=%d, PONG=%d", self.ping, self.pong)

    def regenerate_tokens(self):
        logging.warning("Regenerating tokens")
        self.stored_token = StoredToken.BOTH

    def incarnate(self):
        self.ping += 1
        self.pong = -self.ping
        logging.info("Incarnation complete. New tokens: PING=%d, PONG=%d", self.ping, self.pong)

async def run(socket, send_address, send_port, listen_port, init):
    await asyncio.gather(
        socket.init_outgoing_connection(send_address, send_port, init),
        socket.listen(listen_port),
        socket.handle_messages()
    )

async def main():
    if len(sys.argv) != 5:
        logging.error("Usage: python script.py <listen_port> <send_address> <send_port> <initializer>")
        sys.exit(1)

    listen_port = int(sys.argv[1])
    send_address = sys.argv[2]
    send_port = int(sys.argv[3])
    initializer = int(sys.argv[4])

    socket = MisraSocket()
    await run(socket, send_address, send_port, listen_port, initializer)

if __name__ == "__main__":
    asyncio.run(main())
