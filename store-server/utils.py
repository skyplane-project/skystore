import enum
from sqlalchemy.orm import declarative_base
from sqlalchemy_repr import RepresentableBase

Base = declarative_base(cls=RepresentableBase)


class Status(str, enum.Enum):
    pending = "pending"
    pending_deletion = "pending_deletion"
    ready = "ready"
