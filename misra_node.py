import asyncio
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class TokenType:
    PING_TOKEN = 0
    PONG_TOKEN = 1

class MisraSocket:
    def __init__(self):
        self.initialization()

        self.has_ping = False
        self.has_pong = False

        self.send_writer = None
        self.listen_reader = None
        self.listen_writer = None
        self.m = 0
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
        await asyncio.sleep(3)

        self.has_ping = True
        self.has_pong = True
        await asyncio.gather(
            self.send(self.ping),
            self.send(self.pong)
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

    async def send(self, token):
        if not self.send_writer:
            logging.error("Send connection is not established.")
            return

        if token > 0:
            self.has_ping = False
        elif token < 0:
            self.has_pong = False

        self.m = token
        self.send_writer.write(f"{token}\n".encode())
        await self.send_writer.drain()
        logging.info("Sent token: %d %s", token, "PING" if token > 0 else "PONG")

    async def handle_messages(self):
        while True:
            token = await self.read_channel.get()

            if abs(token) < abs(self.m):
                logging.info("Received old token. Ignoring.")
                continue

            #recive
            if token < 0: #pong
                logging.info("Recived pong %d", token)
                self.has_pong = True
                if token == self.m and not self.has_ping:
                    self.regenerate_tokens(token)
                self.pong = token

            if token > 0: #ping
                logging.info("Recived ping %d", token)
                self.has_ping = True
                if token == self.m and not self.has_pong:
                    self.regenerate_tokens(token)
                self.ping = token
                asyncio.create_task(self.handleCritival())

            if self.has_pong and self.has_ping:
                logging.info("Both tokens acquired, performing incarnation")
                self.incarnate(token)

            #send
            if token < 0: #pong
                await asyncio.sleep(1)
                await self.send(self.pong)

    async def handleCritival(self):
            logging.info("PING token acquired, entering critical section")
            logging.info("------- Entering critical section")
            await asyncio.sleep(10)
            logging.info("------- Exiting critical section")
            await self.send(self.ping) 

    def regenerate_tokens(self, value):
        logging.info("regenerate_tokens")
        self.ping = abs(value)
        self.pong = -self.ping
        if value < 0 :
            asyncio.create_task(self.send(self.ping))
        elif value > 0:
            asyncio.create_task(self.send(self.pong))

    def incarnate(self, value):
        self.ping = abs(value) + 1
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
