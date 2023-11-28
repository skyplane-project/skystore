from datetime import datetime
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    String,
    Float,
)
from sqlalchemy.orm import relationship
from pydantic import BaseModel, Field, NonNegativeInt, validator
from operations.utils.conf import Base, Status
from sqlalchemy.dialects.postgresql import BIGINT
from typing import Dict, List, Literal, Optional


class DBLogicalObject(Base):
    __tablename__ = "logical_objects"

    # NOTE: This id also servers as the version of the logical object, may change to other alternatives name
    id = Column(Integer, primary_key=True, autoincrement=True)

    bucket = Column(String, ForeignKey("logical_buckets.bucket"))
    logical_bucket = relationship("DBLogicalBucket", back_populates="logical_objects")

    key = Column(String)

    size = Column(BIGINT)
    last_modified = Column(DateTime)
    etag = Column(String)
    status = Column(Enum(Status))

    # indicate whether it is a one that being uploaded after version suspended
    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/AddingObjectstoVersionSuspendedBuckets.html
    version_suspended = Column(Boolean, nullable=False, default=False)

    # whether the objects is a delete marker
    delete_marker = Column(Boolean, nullable=False, default=False)

    # NOTE: we are only supporting one upload for now. This can be changed when we are supporting versioning.
    multipart_upload_id = Column(String)
    multipart_upload_parts = relationship(
        "DBLogicalMultipartUploadPart",
        back_populates="logical_object",
        cascade="all, delete, delete-orphan",
    )

    # Add relationship to physical object
    physical_object_locators = relationship(
        "DBPhysicalObjectLocator",
        back_populates="logical_object",
    )


class DBPhysicalObjectLocator(Base):
    __tablename__ = "physical_object_locators"

    id = Column(Integer, primary_key=True, autoincrement=True)

    location_tag = Column(String)
    cloud = Column(String)
    region = Column(String)

    bucket = Column(
        String, ForeignKey("physical_bucket_locators.bucket")
    )  # added ForeignKey

    physical_bucket = relationship(
        "DBPhysicalBucketLocator",
        back_populates="physical_object_locators",
        primaryjoin=(
            "and_(DBPhysicalObjectLocator.bucket==DBPhysicalBucketLocator.bucket, "
            "DBPhysicalObjectLocator.location_tag==DBPhysicalBucketLocator.location_tag)"
        ),
    )

    key = Column(String)
    lock_acquired_ts = Column(DateTime, nullable=True, default=None)
    status = Column(Enum(Status))
    is_primary = Column(Boolean, nullable=False, default=False)

    version_id = Column(String)  # mimic the type and name of the field in S3

    multipart_upload_id = Column(String)
    multipart_upload_parts = relationship(
        "DBPhysicalMultipartUploadPart",
        back_populates="physical_object_locator",
        cascade="all, delete, delete-orphan",
    )

    # Add relationship to logical object
    logical_object_id = Column(
        Integer, ForeignKey("logical_objects.id"), nullable=False
    )

    logical_object = relationship(
        "DBLogicalObject",
        back_populates="physical_object_locators",
        foreign_keys=[logical_object_id],
    )


class DBStatisticsObject(Base):
    __tablename__ = "statistics_table"

    id = Column(Integer, primary_key=True, autoincrement=True)
    requested_region = Column(String)
    client_region = Column(String)
    operation = Column(String)
    latency = Column(Float)
    timestamp = Column(String)
    object_size = Column(BIGINT)


class LocateObjectRequest(BaseModel):
    bucket: str
    key: str
    client_from_region: str
    version_id: Optional[int] = None


class LocateObjectResponse(BaseModel):
    id: int

    tag: str
    cloud: str
    bucket: str
    region: str
    key: str
    version_id: Optional[str] = None  # must be the physical object version id
    version: Optional[int] = None  # must be the logical object version
    size: Optional[NonNegativeInt] = Field(None, minimum=0, format="int64")
    last_modified: Optional[datetime] = None
    etag: Optional[str] = None
    multipart_upload_id: Optional[str] = None


class DBLogicalMultipartUploadPart(Base):
    __tablename__ = "logical_multipart_upload_parts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    logical_object_id = Column(
        Integer, ForeignKey("logical_objects.id"), nullable=False
    )
    logical_object = relationship(
        "DBLogicalObject", back_populates="multipart_upload_parts"
    )

    part_number = Column(Integer)
    etag = Column(String)
    size = Column(BIGINT)


class DBPhysicalMultipartUploadPart(Base):
    __tablename__ = "physical_multipart_upload_parts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    physical_object_locator_id = Column(
        Integer, ForeignKey("physical_object_locators.id"), nullable=False
    )
    physical_object_locator = relationship(
        "DBPhysicalObjectLocator", back_populates="multipart_upload_parts"
    )

    part_number = Column(Integer)
    etag = Column(String)
    size = Column(Integer)


class StartUploadRequest(LocateObjectRequest):
    is_multipart: bool
    # If we are performing a copy, we want to locate the source object physical locations,
    # and only return the locators in the same locations so the client can do "local bucket copy".
    # NOTE: for future, consider whether the bucket is needed here. Should we only do intra-bucket copy?
    copy_src_bucket: Optional[str] = None
    copy_src_key: Optional[str] = None

    # Policy
    policy: Optional[str] = "push"


class StartUploadResponse(BaseModel):
    locators: List[LocateObjectResponse]
    multipart_upload_id: Optional[str] = None

    copy_src_buckets: List[str]
    copy_src_keys: List[str]


class StartWarmupRequest(LocateObjectRequest):
    warmup_regions: List[str]


class StartWarmupResponse(BaseModel):
    src_locator: LocateObjectResponse
    dst_locators: List[LocateObjectResponse]


class PatchUploadIsCompleted(BaseModel):
    # This is called when the PUT operation finishes or upon CompleteMultipartUpload
    id: int
    size: NonNegativeInt = Field(..., minimum=0, format="int64")
    etag: str
    last_modified: datetime
    version_id: Optional[str] = None
    policy: Optional[str] = "push"


class PatchUploadMultipartUploadId(BaseModel):
    # This is called when the CreateMultipartUpload operation finishes
    id: int
    multipart_upload_id: str


class PatchUploadMultipartUploadPart(BaseModel):
    # This is called when the UploadPart operation finishes
    id: int
    part_number: int
    etag: str
    size: NonNegativeInt = Field(..., minimum=0, format="int64")


class ContinueUploadRequest(LocateObjectRequest):
    multipart_upload_id: str

    do_list_parts: bool = False

    copy_src_bucket: Optional[str] = None
    copy_src_key: Optional[str] = None


class ContinueUploadPhysicalPart(BaseModel):
    part_number: int
    etag: str


class ContinueUploadResponse(LocateObjectResponse):
    multipart_upload_id: str

    parts: Optional[List[ContinueUploadPhysicalPart]] = None

    copy_src_bucket: Optional[str] = None
    copy_src_key: Optional[str] = None


class ListObjectRequest(BaseModel):
    bucket: str
    prefix: Optional[str] = None
    start_after: Optional[str] = None
    max_keys: Optional[int] = None


class ObjectResponse(BaseModel):
    bucket: str
    key: str
    size: NonNegativeInt = Field(..., minimum=0, format="int64")
    etag: Optional[str] = None
    last_modified: Optional[datetime] = None
    version_id: Optional[int] = None  # logical object version


class ObjectStatus(BaseModel):
    status: Status


class HeadObjectRequest(BaseModel):
    bucket: str
    key: str
    version_id: Optional[int] = None


class HeadObjectResponse(BaseModel):
    bucket: str
    key: str
    size: NonNegativeInt = Field(..., minimum=0, format="int64")
    etag: str
    last_modified: datetime
    version_id: Optional[int] = None


class MultipartResponse(BaseModel):
    bucket: str
    key: str
    upload_id: str


class ListPartsRequest(BaseModel):
    bucket: str
    key: str
    upload_id: str

    part_number: Optional[int] = None


class LogicalPartResponse(BaseModel):
    part_number: int
    etag: str
    # TODO: remove this size thing, it doesn't matter
    size: NonNegativeInt = Field(..., minimum=0, format="int64")


class HealthcheckResponse(BaseModel):
    status: Literal["OK"]


class DeleteObjectsRequest(BaseModel):
    bucket: str
    object_identifiers: Dict[str, set[int]]
    multipart_upload_ids: Optional[List[str]] = None


class DeleteMarker(BaseModel):
    delete_marker: bool
    version_id: Optional[str] = None


class DeleteObjectsResponse(BaseModel):
    locators: Dict[str, List[LocateObjectResponse]]
    delete_markers: Dict[
        str, DeleteMarker
    ]  # (key, (is_delete_marker, delete_marker_version_id))
    op_type: Dict[str, str]  # (key, op_type={'replace', 'delete', 'add'}])


class DeleteObjectsIsCompleted(BaseModel):
    ids: List[int]
    multipart_upload_ids: Optional[List[str]] = None
    op_type: List[str]  # {'replace', 'delete', 'add'}


class RecordMetricsRequest(BaseModel):
    client_region: str
    requested_region: str
    # read or write
    operation: str
    latency: float
    timestamp: str
    object_size: NonNegativeInt = Field(..., minimum=0, format="int64")

    @validator("operation")
    def c_match(cls, value):
        if value not in ["read", "write"]:
            raise ValueError(
                f"RecordMetrics operation must be read or write, not {value}"
            )
        return value


class ListMetricsRequest(BaseModel):
    client_region: str


class ListMetricsObject(BaseModel):
    client_region: str
    requested_region: str
    operation: str
    latency: float
    timestamp: str
    object_size: NonNegativeInt = Field(..., minimum=0, format="int64")


class ListMetricsResponse(BaseModel):
    metrics: List[ListMetricsObject]
    count: int
