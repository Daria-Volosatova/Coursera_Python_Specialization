import asyncio

dictionary = dict()


def run_server(host, port):
    loop = asyncio.get_event_loop()
    coro = loop.create_server(ClientServerProtocol, host, port)
    server = loop.run_until_complete(coro)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


class ClientServerProtocol(asyncio.Protocol):
    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        self.transport.write(self.process(data.decode('utf-8').strip('\r\n')).encode('utf-8'))
        #chunks = data.decode('utf-8').strip('\r\n').split(' ')

    def process(self, command):
        try:
            chunks = command.split(' ')
            if (chunks[0] == 'get') and (chunks[1] != '') and (len(chunks) == 2):
                return self.process_get(chunks[1])
            elif (chunks[0] == 'put') and (chunks[1] != '') and (chunks[2] != '') and (chunks[3] != '') and (len(chunks) == 4):
                try:
                    chunks[2] = float(chunks[2])
                    chunks[3] = int(chunks[3])
                except:
                    return 'error\nwrong command\n\n'
                return self.process_put(chunks[1], chunks[2], chunks[3])
            else:
                return 'error\nwrong command\n\n'
        except:
            return 'error\nwrong command\n\n'

    def process_put(self, key_name, value, timestamp=None):
        if key_name not in dictionary:
            dictionary[key_name] = list()
        if (timestamp, value) not in dictionary[key_name]:
            flag = 0
            for i in range(len(dictionary[key_name])):
                if dictionary[key_name][i][0] == timestamp:
                    dictionary[key_name][i] = (timestamp, value)
                    flag = 1
            if not flag:
                dictionary[key_name].append((timestamp, value))
                #dictionary[key_name].sort(key=lambda tup: tup[1])
        return 'ok\n\n'


    def process_get(self, key_name):
        res = 'ok\n'
        if key_name == '*':
            for key, values in dictionary.items():
                for value in values:
                    res = res + key + ' ' + str(value[1]) + ' ' + str(value[0]) + '\n'
        elif key_name in dictionary:
            for value in dictionary[key_name]:
                res = res + key_name + ' ' + str(value[1]) + ' ' + str(value[0]) + '\n'
        return res + '\n'






