
from pydantic import BaseModel

class LockRequest(BaseModel):
    lock_consumer_id : str

class UnlockRequest(BaseModel):
    unlock_consumer_id : str

class LockResponse(BaseModel):
    can_lock : bool

class UnlockResponse(BaseModel):
    is_unlocked: bool

