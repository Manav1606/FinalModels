from typing import Union
class sackExceptions(Exception):
    def __init__(self, code: Union[int, str] = 404 ,message = "genric Error"):
        super().__init__(message)
        self.message = message
        self.code = code
    
    def __str__(self):
        return f"[Error {self.code}]: {self.message}"
    