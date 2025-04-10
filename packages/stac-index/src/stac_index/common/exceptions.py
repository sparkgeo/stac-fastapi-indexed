class UriNotFoundException(Exception):
    def __init__(self, uri: str):
        self.uri = uri


class MissingIndexException(Exception):
    pass
