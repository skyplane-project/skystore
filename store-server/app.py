import enum
import logging
import uuid
from datetime import datetime
from typing import Annotated, List, Literal, Optional, Union

from fastapi import Depends, FastAPI, Response, status
from fastapi.routing import APIRoute
from numpy import minimum
from pydantic import BaseConfig, BaseModel, Field, NonNegativeInt, PositiveInt, StrictInt, validate_arguments, validator
from rich.logging import RichHandler
from sqlalchemy import Column, DateTime, Enum, ForeignKey, Integer, String, select, Boolean
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy_repr import RepresentableBase
from sqlalchemy.dialects.postgresql import BIGINT

from conf import TEST_CONFIGURATION


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(filename)s:%(lineno)d - %(message)s",
    datefmt="[%X]",
    handlers=[RichHandler()],
    force=True,
)

import os

LOG_SQL = os.environ.get("LOG_SQL", "false").lower() == "1"

logger = logging.getLogger()

Base = declarative_base(cls=RepresentableBase)
# engine = create_async_engine("sqlite+aiosqlite:///skystore.db", echo=LOG_SQL, future=True)  # , connect_args={"timeout": 15})
engine = create_async_engine("postgresql+asyncpg://ubuntu:ubuntu@localhost:5432/skystore", echo=LOG_SQL, future=True)
async_session = async_sessionmaker(engine, expire_on_commit=False)

app = FastAPI()
conf = TEST_CONFIGURATION


class Status(str, enum.Enum):
    pending = "pending"
    ready = "ready"


class DBLogicalObject(Base):
    __tablename__ = "logical_objects"

    id = Column(Integer, primary_key=True, autoincrement=True)
    bucket = Column(String)
    key = Column(String)

    size = Column(BIGINT)
    last_modified = Column(DateTime)
    etag = Column(String)

    status = Column(Enum(Status))

    # NOTE: we are only supporting one upload for now. This can be changed when we are supporting versioning.
    multipart_upload_id = Column(String)
    multipart_upload_parts = relationship("DBLogicalMultipartUploadPart", back_populates="logical_object")

    physical_object_locators = relationship("DBPhysicalObjectLocator", back_populates="logical_object")


class DBLogicalMultipartUploadPart(Base):
    __tablename__ = "logical_multipart_upload_parts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    logical_object_id = Column(Integer, ForeignKey("logical_objects.id"), nullable=False)
    logical_object = relationship("DBLogicalObject", back_populates="multipart_upload_parts")

    part_number = Column(Integer)
    etag = Column(String)
    size = Column(BIGINT)


class DBPhysicalObjectLocator(Base):
    __tablename__ = "physical_object_locators"

    id = Column(Integer, primary_key=True, autoincrement=True)
    logical_object_id = Column(Integer, ForeignKey("logical_objects.id"), nullable=False)
    logical_object = relationship("DBLogicalObject", back_populates="physical_object_locators")

    location_tag = Column(String)
    cloud = Column(String)
    region = Column(String)
    bucket = Column(String)
    key = Column(String)

    is_primary = Column(Boolean, nullable=False, default=False)

    multipart_upload_id = Column(String)
    multipart_upload_parts = relationship("DBPhysicalMultipartUploadPart", back_populates="physical_object_locator")

    status = Column(Enum(Status))


class DBPhysicalMultipartUploadPart(Base):
    __tablename__ = "physical_multipart_upload_parts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    physical_object_locator_id = Column(Integer, ForeignKey("physical_object_locators.id"), nullable=False)
    physical_object_locator = relationship("DBPhysicalObjectLocator", back_populates="multipart_upload_parts")

    part_number = Column(Integer)
    etag = Column(String)
    size = Column(Integer)


@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # await conn.exec_driver_sql("pragma journal_mode=memory")
        # await conn.exec_driver_sql("pragma synchronous=OFF")


async def get_session() -> AsyncSession:
    async with async_session() as session:
        yield session


DBSession = Annotated[AsyncSession, Depends(get_session)]


class LocateObjectRequest(BaseModel):
    bucket: str
    key: str
    client_from_region: str


class LocateObjectResponse(BaseModel):
    id: int

    tag: str

    cloud: str
    bucket: str
    region: str
    key: str

    size: Optional[PositiveInt] = Field(None, minimum=0, format="int64")
    last_modified: Optional[datetime]
    etag: Optional[str]


@app.post(
    "/locate_object",
    responses={status.HTTP_200_OK: {"model": LocateObjectResponse}, status.HTTP_404_NOT_FOUND: {"description": "Object not found"}},
)
async def locate_object(request: LocateObjectRequest, db: DBSession) -> LocateObjectResponse:
    """Given the logical object information, return one or zero physical object locators."""
    stmt = (
        select(DBPhysicalObjectLocator)
        .join(DBLogicalObject)
        .where(DBLogicalObject.bucket == request.bucket)
        .where(DBLogicalObject.key == request.key)
        .where(DBLogicalObject.status == Status.ready)
    )
    locators = (await db.scalars(stmt)).all()

    if len(locators) == 0:
        return Response(status_code=404, content="Not Found")

    chosen_locator = None
    reason = ""
    for locator in locators:
        if locator.location_tag == request.client_from_region:
            chosen_locator = locator
            reason = "exact match"
            break
    else:
        # find the primary locator
        chosen_locator = next(locator for locator in locators if locator.is_primary)
        reason = "fallback to primary"

    logger.debug(f"locate_object: chosen locator with strategy {reason} out of {len(locators)}, {request} -> {chosen_locator}")

    await db.refresh(chosen_locator, ["logical_object"])

    return LocateObjectResponse(
        id=chosen_locator.id,
        tag=chosen_locator.location_tag,
        cloud=chosen_locator.cloud,
        bucket=chosen_locator.bucket,
        region=chosen_locator.region,
        key=chosen_locator.key,
        size=chosen_locator.logical_object.size,
        last_modified=chosen_locator.logical_object.last_modified,
        etag=chosen_locator.logical_object.etag,
    )


class StartUploadRequest(LocateObjectRequest):
    is_multipart: bool
    # If we are performing a copy, we want to locate the source object physical locations,
    # and only return the locators in the same locations so the client can do "local bucket copy".
    # NOTE: for future, consider whether the bucket is needed here. Should we only do intra-bucket copy?
    copy_src_bucket: Optional[str]
    copy_src_key: Optional[str]


class StartUploadResponse(BaseModel):
    locators: List[LocateObjectResponse]
    multipart_upload_id: Optional[str]

    copy_src_buckets: List[str]
    copy_src_keys: List[str]


@app.post("/start_upload")
async def start_upload(request: StartUploadRequest, db: DBSession) -> StartUploadResponse:
    stmt = (
        select(DBPhysicalObjectLocator)
        .join(DBLogicalObject)
        .where(DBLogicalObject.bucket == request.bucket)
        .where(DBLogicalObject.key == request.key)
        .where(DBLogicalObject.status == Status.ready)
        .where(DBPhysicalObjectLocator.location_tag == request.client_from_region)
    )
    object_already_exists = len((await db.scalars(stmt)).all()) > 0

    if object_already_exists:
        logger.error("This exact object already exists")
        return Response(status_code=409, content="Conflict, object already exists")

    existing_objects_stmt = (
        select(DBPhysicalObjectLocator)
        .join(DBLogicalObject)
        .where(DBLogicalObject.bucket == request.bucket)
        .where(DBLogicalObject.key == request.key)
        .where(DBLogicalObject.status == Status.ready)
    )
    existing_objects = (await db.scalars(existing_objects_stmt)).all()
    existing_tags = set(locator.location_tag for locator in existing_objects)
    primary_exists = any(locator.is_primary for locator in existing_objects)

    if (request.copy_src_bucket is not None) and (request.copy_src_key is not None):
        copy_src_stmt = (
            select(DBPhysicalObjectLocator)
            .join(DBLogicalObject)
            .where(DBLogicalObject.bucket == request.copy_src_bucket)
            .where(DBLogicalObject.key == request.copy_src_key)
            .where(DBLogicalObject.status == Status.ready)
        )
        copy_src_locators = (await db.scalars(copy_src_stmt)).all()
        copy_src_locators_map = {locator.location_tag: locator for locator in copy_src_locators}
        copy_src_locations = set(locator.location_tag for locator in copy_src_locators)
    else:
        copy_src_locations = None

    if len(existing_objects) == 0:
        logical_object = DBLogicalObject(
            bucket=request.bucket,
            key=request.key,
            size=None,
            last_modified=None,
            etag=None,
            status=Status.pending,
            multipart_upload_id=uuid.uuid4().hex if request.is_multipart else None,
        )
        db.add(logical_object)
    else:
        await db.refresh(existing_objects[0], ["logical_object"])
        logical_object = existing_objects[0].logical_object

    upload_to_region_tags = [request.client_from_region] + conf.lookup(request.client_from_region).broadcast_to
    copy_src_buckets = []
    copy_src_keys = []
    if copy_src_locations is not None:
        # We are doing copy here, so we want to only ask the client to perform where the source object exists.
        # This means
        # (1) filter down the desired locality region to the one where src exists
        # (2) if not preferred location, we will ask the client to copy in the region where the object is
        # available.
        upload_to_region_tags = [tag for tag in upload_to_region_tags if tag in copy_src_locations]
        if len(upload_to_region_tags) == 0:
            upload_to_region_tags = list(copy_src_locations)

        copy_src_buckets = [copy_src_locators_map[tag].bucket for tag in upload_to_region_tags]
        copy_src_keys = [copy_src_locators_map[tag].key for tag in upload_to_region_tags]

        logger.debug(
            f"start_upload: copy_src_locations={copy_src_locations}, upload_to_region_tags={upload_to_region_tags}, copy_src_buckets={copy_src_buckets}, copy_src_keys={copy_src_keys}"
        )

    locators = []
    for region_tag in upload_to_region_tags:
        if region_tag in existing_tags:
            continue

        location = conf.lookup(region_tag)
        locators.append(
            DBPhysicalObjectLocator(
                logical_object=logical_object,
                location_tag=region_tag,
                cloud=location.cloud,
                region=location.region,
                bucket=location.bucket,
                key=location.prefix + request.key,
                status=Status.pending,
            )
        )
    if not primary_exists:
        locators[0].is_primary = True

    db.add_all(locators)
    await db.commit()

    logger.debug(f"start_upload: {request} -> {locators}")

    return StartUploadResponse(
        multipart_upload_id=logical_object.multipart_upload_id,
        locators=[
            LocateObjectResponse(
                id=locator.id,
                tag=locator.location_tag,
                cloud=locator.cloud,
                bucket=locator.bucket,
                region=locator.region,
                key=locator.key,
            )
            for locator in locators
        ],
        copy_src_buckets=copy_src_buckets,
        copy_src_keys=copy_src_keys,
    )


class PatchUploadIsCompleted(BaseModel):
    # This is called when the PUT operation finishes or upon CompleteMultipartUpload
    id: int
    size: NonNegativeInt = Field(..., minimum=0, format="int64")
    etag: str
    last_modified: datetime


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


@app.patch("/complete_upload")
async def complete_upload(request: PatchUploadIsCompleted, db: DBSession):
    stmt = select(DBPhysicalObjectLocator).join(DBLogicalObject).where(DBPhysicalObjectLocator.id == request.id)
    physical_locator = await db.scalar(stmt)
    if physical_locator is None:
        logger.error(f"physical locator not found: {request}")
        return Response(status_code=404, content="Not Found")
    await db.refresh(physical_locator, ["logical_object"])

    logger.debug(f"complete_upload: {request} -> {physical_locator}")

    physical_locator.status = Status.ready
    if physical_locator.is_primary:
        physical_locator.logical_object.status = Status.ready
        physical_locator.logical_object.size = request.size
        physical_locator.logical_object.etag = request.etag
        physical_locator.logical_object.last_modified = request.last_modified.replace(tzinfo=None)
    await db.commit()


@app.patch("/set_multipart_id")
async def set_multipart_id(request: PatchUploadMultipartUploadId, db: DBSession):
    stmt = select(DBPhysicalObjectLocator).join(DBLogicalObject).where(DBPhysicalObjectLocator.id == request.id)
    physical_locator = await db.scalar(stmt)
    if physical_locator is None:
        logger.error(f"physical locator not found: {request}")
        return Response(status_code=404, content="Not Found")
    await db.refresh(physical_locator, ["logical_object"])

    logger.debug(f"set_multipart_id: {request} -> {physical_locator}")

    physical_locator.multipart_upload_id = request.multipart_upload_id

    await db.commit()


@app.patch("/append_part")
async def append_part(request: PatchUploadMultipartUploadPart, db: DBSession):
    stmt = select(DBPhysicalObjectLocator).join(DBLogicalObject).where(DBPhysicalObjectLocator.id == request.id)
    physical_locator = await db.scalar(stmt)
    if physical_locator is None:
        logger.error(f"physical locator not found: {request}")
        return Response(status_code=404, content="Not Found")
    await db.refresh(physical_locator, ["logical_object"])

    logger.debug(f"append_part: {request} -> {physical_locator}")

    await db.refresh(physical_locator, ["multipart_upload_parts"])
    physical_locator.multipart_upload_parts.append(
        DBPhysicalMultipartUploadPart(
            part_number=request.part_number,
            etag=request.etag,
            size=request.size,
        )
    )

    if physical_locator.is_primary:
        await db.refresh(physical_locator.logical_object, ["multipart_upload_parts"])
        physical_locator.logical_object.multipart_upload_parts.append(
            DBLogicalMultipartUploadPart(
                part_number=request.part_number,
                etag=request.etag,
                size=request.size,
            )
        )

    await db.commit()


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


@app.post("/continue_upload")
async def continue_upload(request: ContinueUploadRequest, db: DBSession) -> List[ContinueUploadResponse]:
    stmt = (
        select(DBPhysicalObjectLocator)
        .join(DBLogicalObject)
        .where(DBLogicalObject.bucket == request.bucket)
        .where(DBLogicalObject.key == request.key)
        .where(DBLogicalObject.status == Status.pending)
        .where(DBLogicalObject.multipart_upload_id == request.multipart_upload_id)
    )
    locators = (await db.scalars(stmt)).all()
    if len(locators) == 0:
        return Response(status_code=404, content="Not Found")

    copy_src_buckets, copy_src_keys = [], []
    if request.copy_src_bucket is not None and request.copy_src_key is not None:
        physical_src_locators = (
            await db.scalars(
                select(DBPhysicalObjectLocator)
                .join(DBLogicalObject)
                .where(DBLogicalObject.bucket == request.copy_src_bucket)
                .where(DBLogicalObject.key == request.copy_src_key)
                .where(DBLogicalObject.status == Status.ready)
            )
        ).all()
        src_tags = {locator.location_tag for locator in physical_src_locators}
        dst_tags = {locator.location_tag for locator in locators}
        if src_tags != dst_tags:
            return Response(
                status_code=404,
                content=(
                    "Source object was not found in the same region that multipart upload was initiated."
                    f" src_tags={src_tags} dst_tags={dst_tags}"
                ),
            )
        src_map = {locator.location_tag: locator for locator in physical_src_locators}
        for locator in locators:
            copy_src_buckets.append(src_map[locator.location_tag].bucket)
            copy_src_keys.append(src_map[locator.location_tag].key)

    if request.do_list_parts:
        for locator in locators:
            await db.refresh(locator, ["multipart_upload_parts"])

    logger.debug(f"continue_upload: {request} -> {locators}")

    return [
        ContinueUploadResponse(
            id=locator.id,
            tag=locator.location_tag,
            cloud=locator.cloud,
            bucket=locator.bucket,
            region=locator.region,
            key=locator.key,
            multipart_upload_id=locator.multipart_upload_id,
            parts=[
                ContinueUploadPhysicalPart(
                    part_number=part.part_number,
                    etag=part.etag,
                )
                for part in locator.multipart_upload_parts
            ]
            if request.do_list_parts
            else None,
            copy_src_bucket=copy_src_buckets[i] if request.copy_src_bucket is not None else None,
            copy_src_key=copy_src_keys[i] if request.copy_src_key is not None else None,
        )
        for i, locator in enumerate(locators)
    ]


class ListObjectRequest(BaseModel):
    bucket: str
    prefix: Optional[str] = None


class ObjectResponse(BaseModel):
    bucket: str
    key: str
    size: NonNegativeInt = Field(..., minimum=0, format="int64")
    etag: str
    last_modified: datetime


@app.post("/list_objects")
async def list_objects(request: ListObjectRequest, db: DBSession) -> List[ObjectResponse]:
    stmt = select(DBLogicalObject).where(DBLogicalObject.bucket == request.bucket).where(DBLogicalObject.status == Status.ready)
    if request.prefix is not None:
        stmt = stmt.where(DBLogicalObject.key.startswith(request.prefix))
    objects = (await db.scalars(stmt)).all()

    logger.debug(f"list_objects: {request} -> {objects}")

    return [
        ObjectResponse(
            bucket=obj.bucket,
            key=obj.key,
            size=obj.size,
            etag=obj.etag,
            last_modified=obj.last_modified,
        )
        for obj in objects
    ]


class HeadObjectRequest(BaseModel):
    bucket: str
    key: str


class HeadObjectResponse(BaseModel):
    bucket: str
    key: str
    size: NonNegativeInt = Field(..., minimum=0, format="int64")
    etag: str
    last_modified: datetime


@app.post("/head_object")
async def head_object(request: HeadObjectRequest, db: DBSession) -> HeadObjectResponse:
    stmt = (
        select(DBLogicalObject)
        .where(DBLogicalObject.bucket == request.bucket)
        .where(DBLogicalObject.key == request.key)
        .where(DBLogicalObject.status == Status.ready)
    )
    obj = await db.scalar(stmt)

    if obj is None:
        return Response(status_code=404, content="Not Found")

    logger.debug(f"head_object: {request} -> {obj}")

    return HeadObjectResponse(
        bucket=obj.bucket,
        key=obj.key,
        size=obj.size,
        etag=obj.etag,
        last_modified=obj.last_modified,
    )


class MultipartResponse(BaseModel):
    bucket: str
    key: str
    upload_id: str


@app.post("/list_multipart_uploads")
async def list_multipart_uploads(request: ListObjectRequest, db: DBSession) -> List[MultipartResponse]:
    stmt = (
        select(DBLogicalObject)
        .where(DBLogicalObject.bucket == request.bucket)
        .where(DBLogicalObject.key.startswith(request.prefix))
        .where(DBLogicalObject.status == Status.pending)
    )
    objects = (await db.scalars(stmt)).all()

    logger.debug(f"list_multipart_uploads: {request} -> {objects}")

    return [
        MultipartResponse(
            bucket=obj.bucket,
            key=obj.key,
            upload_id=obj.multipart_upload_id,
        )
        for obj in objects
    ]


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


@app.post("/list_parts")
async def list_parts(request: ListPartsRequest, db: DBSession) -> List[LogicalPartResponse]:
    stmt = (
        select(DBLogicalObject)
        .where(DBLogicalObject.bucket == request.bucket)
        .where(DBLogicalObject.key == request.key)
        .where(DBLogicalObject.status == Status.pending)
        .where(DBLogicalObject.multipart_upload_id == request.upload_id)
    )
    objects = (await db.scalars(stmt)).all()
    if len(objects) == 0:
        return Response(status_code=404, content="Not Found")

    assert len(objects) == 1, "should only have one object"
    await db.refresh(objects[0], ["multipart_upload_parts"])

    logger.debug(f"list_parts: {request} -> {objects[0], objects[0].multipart_upload_parts}")

    return [
        LogicalPartResponse(
            part_number=part.part_number,
            etag=part.etag,
            size=part.size,
        )
        for obj in objects
        for part in obj.multipart_upload_parts
        if request.part_number is None or part.part_number == request.part_number
    ]


class HealthcheckResponse(BaseModel):
    status: Literal["OK"]


@app.get("/healthz")
async def healthz() -> HealthcheckResponse:
    return HealthcheckResponse(status="OK")


## Add routes above this function
def use_route_names_as_operation_ids(app: FastAPI) -> None:
    """
    Simplify operation IDs so that generated API clients have simpler function
    names.

    Should be called only after all routes have been added.
    """
    for route in app.routes:
        if isinstance(route, APIRoute):
            route.operation_id = route.name


use_route_names_as_operation_ids(app)
