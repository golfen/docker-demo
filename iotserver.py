from tornado import ioloop, tcpserver, iostream, gen
from tornado.iostream import StreamClosedError
from bitstring import BitArray
import logging
import struct, pdb


logger = logging.getLogger('IoT server app')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('data.log')
console = logging.StreamHandler()
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s-%(levelname)s-%(message)s')
fh.setFormatter(formatter)
console.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(console)


class CenterServer(tcpserver.TCPServer):
    term_dict = {}


    def handle_stream(self, stream, address):
        logger.info('New stream coming from %s' % str(address))
        f = self.handle_registration(stream, address)


    @gen.coroutine
    def handle_registration(self, stream, address):
        try:
            message = yield stream.read_bytes(21)
            reg_data = struct.unpack('>II12sc',message)
        except Exception as e:
            logger.exception('Exception in handle_registration : %s' % e)

        term_id = hex(reg_data[0]) + reg_data[2]
        logger.info('Registered terminal : %s' % term_id)
        self.term_dict[term_id] = stream
        
        if 'control' in message:
            self.control_stream(term_id, stream)
        else:
            y= self.collecting_stream(term_id, stream)
        

    @gen.coroutine
    def send_to_term(self, term_id, message):
        s = self.term_dict.get(term_id, None)
        if not s:
            return

        try:
            yield s.write(message)
        except StreamClosedError:
            logger.exception('Terminal-%s lost connections.' % term_id)
            self.term_dict.pop(term_id)


    @gen.coroutine
    def collecting_stream(self, term_id, stream):
        msg = BitArray('0x7e0x14')

        try:
            while True:
                message = yield stream.read_bytes(2)
                header = struct.unpack('!II', message)
                logger.info('Received dataframe header %s' % hex(ord(message[0])))

                if not header[0]==msg.bytes[0]:
                    print 'not equal'
                    continue

                next_byte_num = header[1]
                logger.info('The data length is %d' % next_byte_num)

                content = yield stream.read_bytes(next_byte_num)
                hcnt = ''
                for byte in content:
                    hcnt += hex(ord(byte))
                logger.info('Received data %s' % hcnt)
                # if message[0] == HEADERS:
                #     body = yield stream.read_bytes(BODY_LEN)
                #     VERIFY AND PROCESS
                #print message
        except StreamClosedError:
            logger.exception('Terminal-%s lost connections.' % term_id)
            self.term_dict.pop(term_id)
        except Exception as e:
            logger.error('Exception in collecting_stream : %s' % str(e))


    @gen.coroutine
    def control_stream(self, term_id, stream):
        cmd = yield stream.read_until('/xfd/xfd')
        cmd = cmd.split('/xfd/xfd')[0]
        try:
            while not 'quit' in cmd:

                if 'list' in cmd:
                    resp = repr(self.term_dict.keys()) + '/resp'
                    yield stream.write(resp)
                elif 'sendto' in cmd:
                    term_id, message = cmd.split()[1:3]
                    self.send_to_term(term_id, message)

                cmd = yield stream.read_until('/xfd/xfd')
                cmd = cmd.split('/xfd/xfd')[0]
        except StreamClosedError:
            logger.exception('Terminal-%s lost connections.' % term_id)
            self.term_dict.pop(term_id)    


server = CenterServer()
#pdb.set_trace()
server.listen(8777)
logger.info('starting server')
ioloop.IOLoop.instance().start()
