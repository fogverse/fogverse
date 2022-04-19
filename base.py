import numpy as np

from .util import bytes_to_numpy, numpy_to_bytes
from pickle import UnpicklingError

class AbstractConsumer:
    def decode(self, data):
        if not getattr(self, 'auto_decode', True):
            return data
        try:
            return bytes_to_numpy(data)
        except (OSError, ValueError, UnpicklingError):
            pass
        try:
            return data.decode()
        except:
            pass
        return data

    def receive(self):
        raise NotImplementedError

    def receive_error(self, *args, **kwargs):
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

    def acked(self, *args, **kwargs):
        pass

    def send(self, data):
        raise NotImplementedError
