from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Literal

from pyredis.protocol import Array


class CommandParserException(Exception):
    def __init__(self, message, request=None):
        self.message = message
        self.request = request
        super().__init__(self.message)


class SetArgs(Enum):
    EX = "EX"
    PX = "PX"
    EXAT = "EXAT"
    PXAT = "PXAT"
    KEEPTTL = "KEEPTTL"
    NX = "NX"
    XX = "XX"
    GET = "GET"


ExpiryType = Literal[SetArgs.EX, SetArgs.PX, SetArgs.EXAT, SetArgs.PXAT]


@dataclass
class ExpiryOptions:
    expiry_type: ExpiryType
    value: int


def get_expiry_time(opts: ExpiryOptions):
    try:
        match opts.expiry_type:
            case SetArgs.EX:
                return datetime.now() + timedelta(seconds=opts.value)
            case SetArgs.PX:
                return datetime.now() + timedelta(milliseconds=opts.value)
            case SetArgs.EXAT:
                return datetime.fromtimestamp(opts.value)
            case SetArgs.PXAT:
                return datetime.fromtimestamp(opts.value // 1000)
            case _:
                raise ValueError(f"No valid expiry argument given `{opts.expiry_type}`")
    except Exception as e:
        raise CommandParserException(
            f"Failed to set expiry from value {opts.value}: {e}"
        )


class ParseSetArgs:
    def __init__(self, request: Array):
        self.args: list = request.decode()[3:]
        self.commands = []
        self.get_flag = False
        self.set_flag = None
        self.expiry_opt: ExpiryOptions | None = None
        self.keep_ttl_flag = False

    def parse_set_args(self):
        i = 0

        while i < len(self.args):
            try:
                arg = SetArgs(self.args[i])
                match arg:
                    case SetArgs.GET:
                        self.get_flag = True
                    case (
                        SetArgs.EX
                        | SetArgs.PX
                        | SetArgs.EXAT
                        | SetArgs.PXAT
                        | SetArgs.KEEPTTL
                    ):
                        if self.expiry_opt or self.keep_ttl_flag:
                            raise CommandParserException(
                                "Cannot use more than one expiry arg."
                            )

                        if arg == SetArgs.KEEPTTL:
                            self.keep_ttl_flag = True
                            continue

                        i += 1
                        expiry_offset = int(self.args[i])
                        if expiry_offset < 0:
                            raise CommandParserException(
                                f"{arg} must be greater than 0"
                            )
                        self.expiry_opt = ExpiryOptions(arg, expiry_offset)
                    case SetArgs.NX | SetArgs.XX:
                        if self.set_flag is not None:
                            raise CommandParserException(
                                "Can only set NX or XX, not both"
                            )
                        self.set_flag = arg
            except ValueError:
                raise CommandParserException(
                    f"The arg `{self.args[i]}` is not valid for SET command. Must be one of {', '.join([v.value for v in SetArgs])}"
                )
            i += 1
        return self

    def opts_exist(self):
        return self.get_flag or self.set_flag or self.expiry_opt or self.keep_ttl_flag
