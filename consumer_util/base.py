from dataclasses import dataclass

BASE_UTF= 'utf-8'

__LOCK_REQUEST_PREFIX = 'lock_request#'
__LOCK_RESPONSE_PREFIX = 'lock_response#'
__UNLOCK_REQUEST_PREFIX = 'unlock_request#'
__UNLOCK_RESPONSE_PREFIX = 'unlock_response#'

def __encode(prefix, data):
    return f"{prefix}{data}".encode(BASE_UTF)

def __decode(prefix, data):
    str_ = data.decode(BASE_UTF)
    is_lock_request  = str_[:len(prefix)] == prefix
    assert is_lock_request
    return str_[len(prefix):]

@dataclass
class LockRequest:
    lock_consumer_id : str

    def encode(self) -> bytes:
        return __encode(__LOCK_REQUEST_PREFIX, self.lock_consumer_id)

    @staticmethod
    def decode(consumer_id : bytes):
        return LockRequest(__decode(__LOCK_REQUEST_PREFIX, consumer_id))

@dataclass
class UnlockRequest:
    unlock_consumer_id : str

    def encode(self) -> bytes:
        return __encode(__UNLOCK_REQUEST_PREFIX, self.unlock_consumer_id)

    @staticmethod
    def decode(consumer_id : bytes):
        return UnlockRequest(__decode(__UNLOCK_REQUEST_PREFIX, consumer_id))

@dataclass
class LockResponse:
    can_lock : bool

    def encode(self):
        return __encode(__LOCK_RESPONSE_PREFIX, self.can_lock)

    @staticmethod
    def decode(can_lock_bytes : bytes):
        decoded_val = __decode(__LOCK_RESPONSE_PREFIX, can_lock_bytes)
        return LockResponse(decoded_val == 'True')

@dataclass
class UnlockResponse:
    is_unlocked: bool

    def encode(self):
        return __encode(__UNLOCK_RESPONSE_PREFIX, self.is_unlocked)

    @staticmethod
    def decode(can_lock_bytes : bytes):
        return __decode(__UNLOCK_RESPONSE_PREFIX, can_lock_bytes)
