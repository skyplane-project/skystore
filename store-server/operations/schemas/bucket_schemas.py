from datetime import datetime
from typing import List, Optional
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    String,
    Index,
)
from sqlalchemy.orm import relationship
from pydantic import BaseModel
from operations.utils.conf import Base, Status, Configuration


class DBLogicalBucket(Base):
    __tablename__ = "logical_buckets"

    id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    bucket = Column(String,unique=True)
    prefix = Column(String)

    # NOTE: do we need status per logical bucket?
    status = Column(Enum(Status))
    creation_date = Column(DateTime)
    version_enabled = Column(Boolean)

    # Add relationship to physical bucket
    physical_bucket_locators = relationship(
        "DBPhysicalBucketLocator", back_populates="logical_bucket"
    )

    # Add relationship to logical object
    logical_objects = relationship("DBLogicalObject", back_populates="logical_bucket")

    __table_args__ = (Index("ix_logical_buckets_bucket_status", "bucket", "status"),)


class DBPhysicalBucketLocator(Base):
    __tablename__ = "physical_bucket_locators"

    id = Column(Integer, primary_key=True, autoincrement=True, unique=True)

    location_tag = Column(String)
    cloud = Column(String)
    region = Column(String)
    bucket = Column(String, unique=True)
    prefix = Column(String, default="")

    lock_acquired_ts = Column(DateTime, nullable=True, default=None)
    status = Column(Enum(Status), default=Status.pending)
    is_primary = Column(
        Boolean, nullable=False, default=False
    )  # TODO: bucket / object-level primary fix; fix prefix
    need_warmup = Column(Boolean, nullable=False, default=False)  # push-based warmup

    # Add relationship to logical bucket
    logical_bucket_id = Column(
        Integer, ForeignKey("logical_buckets.id"), nullable=False
    )
    logical_bucket = relationship(
        "DBLogicalBucket", back_populates="physical_bucket_locators"
    )

    # Add explicit relationship to DBPhysicalObjectLocator
    physical_object_locators = relationship(
        "DBPhysicalObjectLocator", back_populates="physical_bucket"
    )


class LocateBucketRequest(BaseModel):
    bucket: str
    client_from_region: str


class LocateBucketResponse(BaseModel):
    # NOTE: return a physical bucket if any?
    id: int
    tag: str
    cloud: str
    bucket: str
    region: str


class BucketStatus(BaseModel):
    status: Status


class HeadBucketRequest(BaseModel):
    bucket: str


class BucketResponse(BaseModel):
    bucket: str
    creation_date: datetime


class RegisterBucketRequest(BaseModel):
    bucket: str
    config: Configuration
    versioning: bool


class CreateBucketRequest(LocateBucketRequest):
    # warmup regions for push-based warmup
    warmup_regions: Optional[List[str]] = None


class CreateBucketResponse(BaseModel):
    locators: List[LocateBucketResponse]


class CreateBucketIsCompleted(BaseModel):
    id: int
    creation_date: datetime


class DeleteBucketRequest(BaseModel):
    bucket: str


class DeleteBucketResponse(CreateBucketResponse):
    pass


class DeleteBucketIsCompleted(BaseModel):
    id: int


class PutBucketVersioningRequest(BaseModel):
    bucket: str
    versioning: bool
