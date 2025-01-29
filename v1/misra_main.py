import asyncio
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

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
        self.has_ping = False
        self.has_pong = False
        self.read_channel = asyncio.Queue()

    async def send(self, token_type):
        if not self.send_writer:
            logging.error("Send connection is not established.")
            return
        if token_type == TokenType.PING_TOKEN:
            token = self.ping
            self.has_ping = False
        elif token_type == TokenType.PONG_TOKEN:
            token = self.pong
            self.has_pong = False
        else:
            logging.error("Unexpected TOCKEN type.")
            return
        self.m = token
        self.send_writer.write(f"{token}\n".encode())
        await self.send_writer.drain()
        logging.info("Token sent: %s", "PING" if token_type == TokenType.PING_TOKEN else "PONG")

    async def establish_connection(self, send_address, send_port):
        logging.info("Trying to connect to %s:%d", send_address, send_port)
        while True:
            try:
                self.send_reader, self.send_writer = await asyncio.open_connection(send_address, send_port)
                logging.info("Successfully connected to %s:%d", send_address, send_port)
                if self.initializer:
                    await self.send_init_message()
                break
            except ConnectionError as e:
                logging.error("Error connecting to %s:%d: %s. Retrying...", send_address, send_port, e)
                await asyncio.sleep(1)

    async def send_init_message(self):
        logging.info("The init message with tokens were sent.")
        self.has_pong = True
        self.has_ping = True
        await asyncio.sleep(3)
        asyncio.create_task(self.send(TokenType.PING_TOKEN))
        asyncio.create_task(self.send(TokenType.PONG_TOKEN))

    async def listen(self, listen_port):
        server = await asyncio.start_server(self.handle_client, host="0.0.0.0", port=listen_port)
        logging.info("I'm listening on port %d", listen_port)
        async with server:
            await server.serve_forever()

    async def handle_client(self, reader, writer):
        self.listen_reader = reader
        self.listen_writer = writer
        logging.info("Connection established with %s", writer.get_extra_info("peername"))
        asyncio.create_task(self.read_from_connection())


    async def critical_section(self):
        logging.info("PING token received, I will enter to the critical section.")
        logging.info("____Critical section START____")
        await asyncio.sleep(10)
        logging.info("____Critical section END____")
        logging.info("Leaving the critical section.")
        await self.send(TokenType.PING_TOKEN)


    async def handle_messages(self):
        while True:
            token = await self.read_channel.get()
            if abs(token) < abs(self.m):
                logging.info("The token is old and has to be ignored.")
                continue
           
            if token < 0:
                logging.info("PONG token with value %d received.", token)
                self.has_pong = True

            if token > 0:
                logging.info("PING token with value %d received.", token)
                self.has_ping = True

            if token == self.m:
                if self.m > 0 and not self.has_pong:
                    logging.warning("PONG token lost, PING: %s, PONG: %s, m:%s. Regenerating..", self.ping, self.pong, self.m)
                    self.regenerate_tokens(token)
                    await self.send(TokenType.PONG_TOKEN)
                    continue
                elif self.m < 0 and not self.has_ping:
                    logging.warning("PING token lost, PING: %s, PONG: %s, m:%s. Regenerating..", self.ping, self.pong, self.m)
                    self.regenerate_tokens(token)
                    await self.send(TokenType.PING_TOKEN)
                    continue

            if token > 0:
                self.ping = token
                asyncio.create_task(self.critical_section())
            
            if token < 0:
                self.pong = token

            if self.has_ping and self.has_pong:
                logging.info("BOTH tokens received. PING: %s, PONG: %s New incarnation of the tockens.", str(self.ping), str(self.pong))
                self.incarnate()
            
            if self.has_pong:
                await asyncio.sleep(1)
                await self.send(TokenType.PONG_TOKEN)

    def regenerate_tokens(self, token):
        self.ping = abs(token)
        self.pong = -self.ping

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

    initiator = int(sys.argv[4])
    socket = MisraSocket(initiator)

    if initiator == True:
        logging.info("My role is the Initiator.")
    await asyncio.gather(
        socket.establish_connection(send_address, send_port),
        socket.listen(listen_port),
        socket.handle_messages()
    )


if __name__ == "__main__":
    asyncio.run(main())
