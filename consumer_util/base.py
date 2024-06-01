from dataclasses import dataclass

BASE_UTF= 'utf-8'

@dataclass
class LockRequest:
    lock_consumer_id : str

    def encode(self) -> bytes:
        return self.lock_consumer_id.encode(BASE_UTF)

    @staticmethod
    def decode(consumer_id : bytes):
        return LockRequest(consumer_id.decode(BASE_UTF))
        

@dataclass
class UnlockRequest:
    unlock_consumer_id : str

    def encode(self) -> bytes:
        return self.unlock_consumer_id.encode(BASE_UTF)

    @staticmethod
    def decode(consumer_id : bytes):
        return LockRequest(consumer_id.decode(BASE_UTF))
        
@dataclass
class LockResponse:
    can_lock : bool

    def encode(self):
        str_repr = 'True' if self.can_lock else 'False'
        return str_repr.encode(BASE_UTF)

    @staticmethod
    def decode(can_lock_bytes : bytes):
        decoded_repr = can_lock_bytes.decode(BASE_UTF)
        return LockResponse(decoded_repr == 'True')

@dataclass
class UnlockResponse:
    is_unlocked: bool

    def encode(self):
        str_repr = 'True' if self.is_unlocked else 'False'
        return str_repr.encode(BASE_UTF)

    @staticmethod
    def decode(can_lock_bytes : bytes):
        decoded_repr = can_lock_bytes.decode(BASE_UTF)
        return UnlockResponse(decoded_repr == 'True')
