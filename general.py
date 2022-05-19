import traceback

def _get_func(obj, func_name):
    func = getattr(obj, func_name, None)
    return func if callable(func) else None

def _call_func(obj, func_name):
    func = _get_func(obj, func_name)
    return func() if func is not None else None

async def _call_func_async(obj, func_name):
    coro = _call_func(obj, func_name)
    return await coro if coro is not None else None

class Runnable:
    def on_error(self, _):
        traceback.print_exc()

    def process(self, data):
        return data

    async def start(self):
        if getattr(self, '_started', False) == True: return
        await _call_func_async(self, 'start_consumer')
        await _call_func_async(self, 'start_producer')
        self._started = True

    async def run(self):
        await self.start()
        try:
            while True:
                self.message = await self.receive()
                if self.message is None: continue

                # kafka and opencv consumer compatibility
                getvalue = getattr(self.message, 'value', None)
                if getvalue is None:
                    value = self.message
                elif callable(getvalue):
                    value = self.message.value()
                else:
                    value = self.message.value

                data = self.decode(value)
                result = self.process(data)
                result_bytes = self.encode(result)
                await self.send(result_bytes)
        except Exception as e:
            self.on_error(e)
        finally:
            await _call_func_async(self, 'close_producer')
            await _call_func_async(self, 'close_consumer')
