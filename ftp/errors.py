__all__ = (
    "AIOFTPException",
    "PathIOError",
    "NoAvailablePort",
)

class AIOFTPException(Exception):
    pass

class PathIOError(AIOFTPException):
    def __init__(self, *args, reason=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.reason = reason

class NoAvailablePort(AIOFTPException, OSError):
    pass