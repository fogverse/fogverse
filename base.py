import numpy as np

from .util import bytes_to_numpy, numpy_to_bytes
from pickle import UnpicklingError

class AbstractConsumer:
    async def start_consumer(self):
        pass

    def receive_error(self, *args, **kwargs):
        pass

    async def receive(self):
        raise NotImplementedError

    def decode(self, data):
        if not getattr(self, 'auto_decode', True):
            return data
        if getattr(self, 'consumer', None) is not None and \
            'ConsumerStorage' in \
                map(lambda x: x.__name__, type(self.consumer).mro()):
            self.message = data['message']
            self._message_extra = data.get('extra',{})
            data = data['data']
        try:
            return bytes_to_numpy(data)
        except (OSError, ValueError, UnpicklingError):
            pass
        try:
            return data.decode()
        except:
            pass
        return data

    async def close_consumer(self):
        pass

class AbstractProducer:
    def encode(self, data):
        if not getattr(self, 'auto_encode', True):
            return data
        if isinstance(data, str):
            return data.encode()
        if isinstance(data, (list, tuple)):
            data = np.array(data)
        if type(data).__name__ == 'Tensor':
            data = data.cpu().numpy()
        if isinstance(data, np.ndarray):
            return numpy_to_bytes(data)
        return bytes(data)

    async def start_producer(self):
        pass

    async def close_producer(self):
        pass

    async def send(self, data):
        raise NotImplementedError
