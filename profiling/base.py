class AbstractProfiling:
    def _before_start(self,*args,**kwargs) -> None:...
    def _after_start(self,*args,**kwargs) -> None:...
    def _before_receive(self,*args,**kwargs) -> None:...
    def _after_receive(self,*args,**kwargs) -> None:...
    def _before_decode(self,*args,**kwargs) -> None:...
    def _after_decode(self,*args,**kwargs) -> None:...
    def _before_process(self,*args,**kwargs) -> None:...
    def _after_process(self,*args,**kwargs) -> None:...
    def _before_encode(self,*args,**kwargs) -> None:...
    def _after_encode(self,*args,**kwargs) -> None:...
    def _before_send(self,*args,**kwargs) -> None:...
    def _after_send(self,*args,**kwargs) -> None:...
    def _before_close(self,*args,**kwargs) -> None:...
    def _after_close(self,*args,**kwargs) -> None:...
