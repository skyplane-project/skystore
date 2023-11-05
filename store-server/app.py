from datetime import datetime, timedelta
import asyncio
import logging
import uuid
from utils import Base
from typing import Annotated, List
from sqlalchemy.orm import joinedload
import os
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Response, status
from fastapi.routing import APIRoute
from rich.logging import RichHandler
from itertools import zip_longest
from sqlalchemy import select, update
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from conf import (
    TEST_CONFIGURATION,
    DEFAULT_INIT_REGIONS,
    DEFAULT_SKYSTORE_BUCKET_PREFIX,
)
from utils import Status
from bucket_models import (
    DBLogicalBucket,
    DBPhysicalBucketLocator,
    RegisterBucketRequest,
    LocateBucketRequest,
    LocateBucketResponse,
    HeadBucketRequest,
    BucketResponse,
    BucketStatus,
    CreateBucketRequest,
    CreateBucketResponse,
    CreateBucketIsCompleted,
    DeleteBucketRequest,
    DeleteBucketResponse,
    DeleteBucketIsCompleted,
)

from object_models import (
    DBLogicalObject,
    DBPhysicalObjectLocator,
    LocateObjectRequest,
    LocateObjectResponse,
    DBLogicalMultipartUploadPart,
    DBPhysicalMultipartUploadPart,
    StartUploadRequest,
    StartUploadResponse,
    StartWarmupRequest,
    StartWarmupResponse,
    PatchUploadIsCompleted,
    PatchUploadMultipartUploadId,
    PatchUploadMultipartUploadPart,
    ContinueUploadRequest,
    ContinueUploadPhysicalPart,
    ContinueUploadResponse,
    ListObjectRequest,
    ObjectResponse,
    ObjectStatus,
    HeadObjectRequest,
    HeadObjectResponse,
    MultipartResponse,
    ListPartsRequest,
    LogicalPartResponse,
    HealthcheckResponse,
    DeleteObjectsRequest,
    DeleteObjectsResponse,
    DeleteObjectsIsCompleted,
)

background_tasks = set()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(filename)s:%(lineno)d - %(message)s",
    datefmt="[%X]",
    handlers=[RichHandler()],
    force=True,
)

LOG_SQL = os.environ.get("LOG_SQL", "false").lower() == "1"

logger = logging.getLogger()

engine = create_async_engine(
    "sqlite+aiosqlite:///skystore.db",
    echo=LOG_SQL,
    future=True,
)
# engine = create_async_engine(
#     "postgresql+asyncpg://ubuntu:ubuntu@localhost:5432/skystore",
#     echo=LOG_SQL,
#     future=True,
# )
async_session = async_sessionmaker(engine, expire_on_commit=False)

app = FastAPI()
app.openapi_version = "3.0.2"
conf = TEST_CONFIGURATION
init_region_tags = DEFAULT_INIT_REGIONS
skystore_bucket_prefix = os.getenv(
    "SKYSTORE_BUCKET_PREFIX", DEFAULT_SKYSTORE_BUCKET_PREFIX
)

load_dotenv()

stop_task_flag = asyncio.Event()


@app.on_event("startup")
async def startup():
    global init_region_tags

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # await conn.exec_driver_sql("pragma journal_mode=memory")
        # await conn.exec_driver_sql("pragma synchronous=OFF")

    task = asyncio.create_task(rm_lock_on_timeout(10))
    background_tasks.add(task)

    if os.getenv("INIT_REGIONS"):
        init_region_tags = os.getenv("INIT_REGIONS").split(",")


@app.on_event("shutdown")
async def shutdown_event():
    # Set the flag to signal the background task to stop
    stop_task_flag.set()
    background_tasks.discard


async def get_session() -> AsyncSession:
    async with async_session() as session:
        yield session


DBSession = Annotated[AsyncSession, Depends(get_session)]


@app.post("/register_buckets")
async def register_buckets(request: RegisterBucketRequest, db: DBSession) -> Response:
    stmt = select(DBLogicalBucket).where(DBLogicalBucket.bucket == request.bucket)
    existing_logical_bucket = await db.scalar(stmt)

    if existing_logical_bucket:
        logger.error("Bucket with this name already exists")
        return Response(status_code=409, content="Conflict, bucket already exists")

    logical_bucket = DBLogicalBucket(
        bucket=request.bucket,
        prefix="",  # TODO: integrate prefix
        status=Status.ready,
        creation_date=datetime.utcnow(),
    )
    db.add(logical_bucket)

    added_loc_tags = set()
    for location in request.config.physical_locations:
        physical_bucket_locator = DBPhysicalBucketLocator(
            logical_bucket=logical_bucket,
            location_tag=location.name,
            cloud=location.cloud,
            region=location.region,
            bucket=location.bucket,
            prefix="",  # location.prefix + "/",
            lock_acquired_ts=None,
            status=Status.ready,
            is_primary=location.is_primary,  # TODO: assume one primary must be specified for now, need to enforce this
            need_warmup=location.need_warmup,
        )
        db.add(physical_bucket_locator)
        added_loc_tags.add(location.name)

    for location_tag in init_region_tags:
        if location_tag not in added_loc_tags:
            cloud, region = location_tag.split(":")
            physical_bucket_locator = DBPhysicalBucketLocator(
                logical_bucket=logical_bucket,
                location_tag=cloud + ":" + region,
                cloud=cloud,
                region=region,
                bucket=f"{skystore_bucket_prefix}-{region}",
                prefix="",  # TODO: integrate with prefix
                lock_acquired_ts=None,
                status=Status.ready,
                is_primary=False,
                need_warmup=False,
            )
            db.add(physical_bucket_locator)

    await db.commit()

    return Response(
        status_code=200,
        content="Logical bucket and physical locations have been registered",
    )


@app.post("/start_create_bucket")
async def start_create_bucket(
    request: CreateBucketRequest, db: DBSession
) -> CreateBucketResponse:
    stmt = select(DBLogicalBucket).where(DBLogicalBucket.bucket == request.bucket)
    existing_logical_bucket = await db.scalar(stmt)

    if existing_logical_bucket:
        logger.error("Bucket with this name already exists")
        return Response(status_code=409, content="Conflict, bucket already exists")

    logical_bucket = DBLogicalBucket(
        bucket=request.bucket,
        prefix="",  # TODO: integrate prefix
        status=Status.pending,
        creation_date=datetime.utcnow(),
    )
    db.add(logical_bucket)

    # warmup_regions: regions to upload warmup objects to upon writes
    warmup_regions = request.warmup_regions if request.warmup_regions else []
    upload_to_region_tags = list(
        set(init_region_tags + [request.client_from_region] + warmup_regions)
    )

    bucket_locators = []

    for region_tag in upload_to_region_tags:
        cloud, region = region_tag.split(":")
        physical_bucket_name = f"{skystore_bucket_prefix}-{region}"  # NOTE: might need another naming scheme

        bucket_locator = DBPhysicalBucketLocator(
            logical_bucket=logical_bucket,
            location_tag=region_tag,
            cloud=cloud,
            region=region,
            bucket=physical_bucket_name,
            prefix="",  # TODO: integrate prefix, e.x. logical_bucket.bucket + "/"
            lock_acquired_ts=datetime.utcnow(),
            status=Status.pending,
            is_primary=(
                region_tag == request.client_from_region
            ),  # set primary where client is from if need to newly create a SkyStorage bucket
            need_warmup=(region_tag in warmup_regions),
        )
        bucket_locators.append(bucket_locator)

    db.add_all(bucket_locators)
    await db.commit()

    logger.debug(f"start_create_bucket: {request} -> {bucket_locators}")

    return CreateBucketResponse(
        locators=[
            LocateBucketResponse(
                id=locator.id,
                tag=locator.location_tag,
                cloud=locator.cloud,
                bucket=locator.bucket,
                region=locator.region,
            )
            for locator in bucket_locators
        ],
    )


@app.patch("/complete_create_bucket")
async def complete_create_bucket(request: CreateBucketIsCompleted, db: DBSession):
    stmt = select(DBPhysicalBucketLocator).where(
        DBPhysicalBucketLocator.id == request.id
    )
    physical_locator = await db.scalar(stmt)
    if physical_locator is None:
        logger.error(f"physical locator not found: {request}")
        return Response(status_code=404, content="Physical Bucket Not Found")
    await db.refresh(physical_locator, ["logical_bucket"])

    logger.debug(f"complete_create_bucket: {request} -> {physical_locator}")

    physical_locator.status = Status.ready
    physical_locator.lock_acquired_ts = None
    if physical_locator.is_primary:
        physical_locator.logical_bucket.status = Status.ready
        physical_locator.logical_bucket.creation_date = request.creation_date.replace(
            tzinfo=None
        )

    await db.commit()


@app.post("/start_delete_bucket")
async def start_delete_bucket(
    request: DeleteBucketRequest, db: DBSession
) -> DeleteBucketResponse:
    logical_bucket_stmt = (
        select(DBLogicalBucket)
        .options(joinedload(DBLogicalBucket.logical_objects))
        .options(joinedload(DBLogicalBucket.physical_bucket_locators))
        .where(DBLogicalBucket.bucket == request.bucket)
    )
    logical_bucket = await db.scalar(logical_bucket_stmt)

    if logical_bucket is None:
        return Response(status_code=404, content="Bucket not found")

    if logical_bucket.status not in Status.ready or logical_bucket.logical_objects:
        return Response(
            status_code=409,
            content="Bucket is not ready for deletion, or has objects in it",
        )

    locators = []
    for locator in logical_bucket.physical_bucket_locators:
        if locator.status not in Status.ready:
            logger.error(
                f"Cannot delete physical bucket. Current status is {locator.status}"
            )
            return Response(
                status_code=409,
                content="Cannot delete physical bucket in current state",
            )

        locator.status = Status.pending_deletion
        locator.lock_acquired_ts = datetime.utcnow()
        locators.append(
            LocateBucketResponse(
                id=locator.id,
                tag=locator.location_tag,
                cloud=locator.cloud,
                bucket=locator.bucket,
                region=locator.region,
            )
        )

    logical_bucket.status = Status.pending_deletion

    try:
        await db.commit()
    except Exception as e:
        logger.error(f"Error occurred while committing changes: {e}")
        return Response(status_code=500, content="Error committing changes")

    logger.debug(f"start_delete_bucket: {request} -> {logical_bucket}")

    return DeleteBucketResponse(locators=locators)


@app.patch("/complete_delete_bucket")
async def complete_delete_bucket(request: DeleteBucketIsCompleted, db: DBSession):
    # TODO: need to deal with partial failures
    physical_locator_stmt = select(DBPhysicalBucketLocator).where(
        DBPhysicalBucketLocator.id == request.id
    )
    physical_locator = await db.scalar(physical_locator_stmt)

    if physical_locator is None:
        logger.error(f"physical locator not found: {request}")
        return Response(status_code=404, content="Physical Bucket Not Found")

    await db.refresh(physical_locator, ["logical_bucket"])

    logger.debug(f"complete_delete_bucket: {request} -> {physical_locator}")

    if physical_locator.status != Status.pending_deletion:
        return Response(
            status_code=409, content="Physical bucket is not marked for deletion"
        )

    # Delete the physical locator
    await db.delete(physical_locator)

    # Check if there are any remaining physical locators for the logical bucket
    remaining_physical_locators_stmt = select(DBPhysicalBucketLocator).where(
        DBPhysicalBucketLocator.logical_bucket_id == physical_locator.logical_bucket.id
    )
    remaining_physical_locators = await db.execute(remaining_physical_locators_stmt)
    if not remaining_physical_locators.all():
        await db.delete(physical_locator.logical_bucket)

    try:
        await db.commit()
    except Exception as e:
        logger.error(f"Error occurred while committing changes: {e}")
        return Response(status_code=500, content="Error committing changes")


@app.post("/start_delete_objects")
async def start_delete_objects(
    request: DeleteObjectsRequest, db: DBSession
) -> DeleteObjectsResponse:
    if request.multipart_upload_ids and len(request.keys) != len(
        request.multipart_upload_ids
    ):
        return Response(
            status_code=400,
            content="Mismatched lengths for ids and multipart_upload_ids",
        )

    locator_dict = {}
    for key, multipart_upload_id in zip_longest(
        request.keys, request.multipart_upload_ids or []
    ):
        if multipart_upload_id:
            stmt = (
                select(DBLogicalObject)
                .options(joinedload(DBLogicalObject.physical_object_locators))
                .where(DBLogicalObject.bucket == request.bucket)
                .where(DBLogicalObject.key == key)
                .where(
                    DBLogicalObject.status == Status.ready
                    or DBLogicalObject.status == Status.pending
                )
                .where(DBLogicalObject.multipart_upload_id == multipart_upload_id)
            )
        else:
            stmt = (
                select(DBLogicalObject)
                .options(joinedload(DBLogicalObject.physical_object_locators))
                .where(DBLogicalObject.bucket == request.bucket)
                .where(DBLogicalObject.key == key)
                .where(DBLogicalObject.status == Status.ready)
            )
        logical_obj = await db.scalar(stmt)
        if logical_obj is None:
            return Response(status_code=404, content="Object not found")

        locators = []
        for physical_locator in logical_obj.physical_object_locators:
            if physical_locator.status not in Status.ready and not multipart_upload_id:
                logger.error(
                    f"Cannot delete physical object. Current status is {physical_locator.status}"
                )
                return Response(
                    status_code=409,
                    content="Cannot delete physical object in current state",
                )

            physical_locator.status = Status.pending_deletion
            physical_locator.lock_acquired_ts = datetime.utcnow()
            locators.append(
                LocateObjectResponse(
                    id=physical_locator.id,
                    tag=physical_locator.location_tag,
                    cloud=physical_locator.cloud,
                    bucket=physical_locator.bucket,
                    region=physical_locator.region,
                    key=physical_locator.key,
                    size=physical_locator.logical_object.size,
                    last_modified=physical_locator.logical_object.last_modified,
                    etag=physical_locator.logical_object.etag,
                    multipart_upload_id=physical_locator.multipart_upload_id,
                )
            )

        logical_obj.status = Status.pending_deletion

        try:
            await db.commit()
        except Exception as e:
            logger.error(f"Error occurred while committing changes: {e}")
            return Response(status_code=500, content="Error committing changes")

        logger.debug(f"start_delete_object: {request} -> {logical_obj}")

        locator_dict[key] = locators

    return DeleteObjectsResponse(locators=locator_dict)


@app.patch("/complete_delete_objects")
async def complete_delete_objects(request: DeleteObjectsIsCompleted, db: DBSession):
    # TODO: need to deal with partial failures
    if request.multipart_upload_ids and len(request.ids) != len(
        request.multipart_upload_ids
    ):
        return Response(
            status_code=400,
            content="Mismatched lengths for ids and multipart_upload_ids",
        )

    for id, multipart_upload_id in zip_longest(
        request.ids, request.multipart_upload_ids or []
    ):
        physical_locator_stmt = (
            select(DBPhysicalObjectLocator)
            .where(DBPhysicalObjectLocator.id == id)
            .where(
                DBPhysicalObjectLocator.multipart_upload_id == multipart_upload_id
                if multipart_upload_id
                else True
            )
        )

        physical_locator = await db.scalar(physical_locator_stmt)

        if physical_locator is None:
            logger.error(f"physical locator not found: {request}")
            return Response(status_code=404, content="Physical Object Not Found")

        await db.refresh(physical_locator, ["logical_object"])

        logger.debug(f"complete_delete_object: {request} -> {physical_locator}")

        if physical_locator.status != Status.pending_deletion:
            return Response(
                status_code=409, content="Physical object is not marked for deletion"
            )

        await db.delete(physical_locator)

        remaining_physical_locators_stmt = select(DBPhysicalObjectLocator).where(
            DBPhysicalObjectLocator.logical_object_id
            == physical_locator.logical_object.id
        )
        remaining_physical_locators = await db.execute(remaining_physical_locators_stmt)
        if not remaining_physical_locators.all():
            await db.delete(physical_locator.logical_object)

        try:
            await db.commit()
        except Exception as e:
            logger.error(f"Error occurred while committing changes: {e}")
            return Response(status_code=500, content="Error committing changes")


@app.post("/head_bucket")
async def head_bucket(request: HeadBucketRequest, db: DBSession):
    stmt = select(DBLogicalBucket).where(
        DBLogicalBucket.bucket == request.bucket, DBLogicalBucket.status == Status.ready
    )
    bucket = await db.scalar(stmt)

    if bucket is None:
        return Response(status_code=404, content="Not Found")

    logger.debug(f"head_bucket: {request} -> {bucket}")

    return Response(
        status_code=200,
        content="Bucket exists",
    )


@app.post(
    "/locate_bucket",
    responses={
        status.HTTP_200_OK: {"model": LocateBucketResponse},
        status.HTTP_404_NOT_FOUND: {"description": "Bucket not found"},
    },
)
async def locate_bucket(
    request: LocateBucketRequest, db: DBSession
) -> LocateBucketResponse:
    """Given the bucket name, return one or zero physical bucket locators."""
    stmt = (
        select(DBPhysicalBucketLocator)
        .join(DBLogicalBucket)
        .where(DBLogicalBucket.bucket == request.bucket)
        .where(DBLogicalBucket.status == Status.ready)
    )
    locators = (await db.scalars(stmt)).all()

    if len(locators) == 0:
        return Response(status_code=404, content="Bucket Not Found")

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

    logger.debug(
        f"locate_bucket: chosen locator with strategy {reason} out of {len(locators)}, {request} -> {chosen_locator}"
    )

    await db.refresh(chosen_locator, ["logical_bucket"])

    return LocateBucketResponse(
        id=chosen_locator.id,
        tag=chosen_locator.location_tag,
        cloud=chosen_locator.cloud,
        bucket=chosen_locator.bucket,
        region=chosen_locator.region,
    )


@app.post(
    "/locate_object",
    responses={
        status.HTTP_200_OK: {"model": LocateObjectResponse},
        status.HTTP_404_NOT_FOUND: {"description": "Object not found"},
    },
)
async def locate_object(
    request: LocateObjectRequest, db: DBSession
) -> LocateObjectResponse:
    """Given the logical object information, return one or zero physical object locators."""
    stmt = (
        select(DBPhysicalObjectLocator)
        .join(DBLogicalObject)
        .where(DBLogicalObject.bucket == request.bucket)
        .where(DBLogicalObject.key == request.key)
        .where(DBLogicalObject.status == Status.ready)
        .where(DBPhysicalObjectLocator.status == Status.ready)
    )
    locators = (await db.scalars(stmt)).all()
    if len(locators) == 0:
        return Response(status_code=404, content="Object Not Found")

    chosen_locator = None
    reason = ""

    # if request.get_primary:
    #     chosen_locator = next(locator for locator in locators if locator.is_primary)
    #     reason = "exact match (primary)"
    # else:
    for locator in locators:
        if locator.location_tag == request.client_from_region:
            chosen_locator = locator
            reason = "exact match"
            break

    if chosen_locator is None:
        # find the primary locator
        chosen_locator = next(locator for locator in locators if locator.is_primary)
        reason = "fallback to primary"

    logger.debug(
        f"locate_object: chosen locator with strategy {reason} out of {len(locators)}, {request} -> {chosen_locator}"
    )

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


@app.post("/start_warmup")
async def start_warmup(
    request: StartWarmupRequest, db: DBSession
) -> StartWarmupResponse:
    """Given the logical object information and warmup regions, return one or zero physical object locators."""
    stmt = (
        select(DBPhysicalObjectLocator)
        .options(joinedload(DBPhysicalObjectLocator.logical_object))
        .join(DBLogicalObject)
        .where(DBLogicalObject.bucket == request.bucket)
        .where(DBLogicalObject.key == request.key)
        .where(DBLogicalObject.status == Status.ready)
    )
    locators = (await db.scalars(stmt)).all()

    if not locators:
        return Response(status_code=404, content="Object Not Found")

    # Transfer from primary locator
    primary_locator = next(locator for locator in locators if locator.is_primary)
    if primary_locator is None:
        logger.error("No primary locator found.")
        return Response(status_code=500, content="Internal Server Error")

    # TODO: at what granularity do we want to do this? per bucket? per object?
    logical_bucket = (
        await db.execute(
            select(DBLogicalBucket)
            .options(selectinload(DBLogicalBucket.physical_bucket_locators))
            .where(DBLogicalBucket.bucket == request.bucket)
        )
    ).scalar_one_or_none()

    # Transfer to warmup regions
    secondary_locators = []
    for region_tag in [
        region for region in request.warmup_regions if region != primary_locator.region
    ]:
        physical_bucket_locator = next(
            (
                pbl
                for pbl in logical_bucket.physical_bucket_locators
                if pbl.location_tag == region_tag
            ),
            None,
        )
        if not physical_bucket_locator:
            logger.error(
                f"No physical bucket locator found for warmup region: {region_tag}"
            )
            return Response(
                status_code=500,
                content=f"No physical bucket locator found for warmup {region_tag}",
            )
        secondary_locator = DBPhysicalObjectLocator(
            logical_object=primary_locator.logical_object,
            location_tag=region_tag,
            cloud=physical_bucket_locator.cloud,
            region=physical_bucket_locator.region,
            bucket=physical_bucket_locator.bucket,
            key=physical_bucket_locator.prefix + request.key,
            status=Status.pending,
            is_primary=False,
        )
        secondary_locators.append(secondary_locator)
        db.add(secondary_locator)

    for locator in secondary_locators:
        locator.status = Status.pending

    await db.commit()
    return StartWarmupResponse(
        src_locator=LocateObjectResponse(
            id=primary_locator.id,
            tag=primary_locator.location_tag,
            cloud=primary_locator.cloud,
            bucket=primary_locator.bucket,
            region=primary_locator.region,
            key=primary_locator.key,
        ),
        dst_locators=[
            LocateObjectResponse(
                id=locator.id,
                tag=locator.location_tag,
                cloud=locator.cloud,
                bucket=locator.bucket,
                region=locator.region,
                key=locator.key,
            )
            for locator in secondary_locators
        ],
    )


@app.post("/start_upload")
async def start_upload(
    request: StartUploadRequest, db: DBSession
) -> StartUploadResponse:
    existing_objects_stmt = (
        select(DBPhysicalObjectLocator)
        .join(DBLogicalObject)
        .where(DBLogicalObject.bucket == request.bucket)
        .where(DBLogicalObject.key == request.key)
        .where(DBLogicalObject.status == Status.ready)
    )
    existing_objects = (await db.scalars(existing_objects_stmt)).all()
    # Parse results for the object_already_exists check
    object_already_exists = any(
        locator.location_tag == request.client_from_region
        for locator in existing_objects
    )

    if object_already_exists:
        logger.error("This exact object already exists")
        return Response(status_code=409, content="Conflict, object already exists")
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
        copy_src_locators_map = {
            locator.location_tag: locator for locator in copy_src_locators
        }
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

    # Old version: look up config file
    # upload_to_region_tags = [request.client_from_region] + conf.lookup(request.client_from_region).broadcast_to

    logical_bucket = (
        await db.execute(
            select(DBLogicalBucket)
            .options(selectinload(DBLogicalBucket.physical_bucket_locators))
            .where(DBLogicalBucket.bucket == request.bucket)
        )
    ).scalar_one_or_none()
    physical_bucket_locators = logical_bucket.physical_bucket_locators

    if primary_exists:
        # Assume that physical bucket locators for this region already exists and we don't need to create them
        upload_to_region_tags = [request.client_from_region]
    else:
        # Push-based: upload to primary region and broadcast to other regions marked with need_warmup
        upload_to_region_tags = [
            locator.location_tag
            for locator in physical_bucket_locators
            if locator.is_primary or locator.need_warmup
        ]

    copy_src_buckets = []
    copy_src_keys = []
    if copy_src_locations is not None:
        # We are doing copy here, so we want to only ask the client to perform where the source object exists.
        # This means
        # (1) filter down the desired locality region to the one where src exists
        # (2) if not preferred location, we will ask the client to copy in the region where the object is
        # available.
        upload_to_region_tags = [
            tag for tag in upload_to_region_tags if tag in copy_src_locations
        ]
        if len(upload_to_region_tags) == 0:
            upload_to_region_tags = list(copy_src_locations)

        copy_src_buckets = [
            copy_src_locators_map[tag].bucket for tag in upload_to_region_tags
        ]
        copy_src_keys = [
            copy_src_locators_map[tag].key for tag in upload_to_region_tags
        ]

        logger.debug(
            f"start_upload: copy_src_locations={copy_src_locations}, "
            f"upload_to_region_tags={upload_to_region_tags}, "
            f"copy_src_buckets={copy_src_buckets}, "
            f"copy_src_keys={copy_src_keys}"
        )

    locators = []
    for region_tag in upload_to_region_tags:
        if region_tag in existing_tags:
            continue

        physical_bucket_locator = next(
            (pbl for pbl in physical_bucket_locators if pbl.location_tag == region_tag),
            None,
        )
        if physical_bucket_locator is None:
            logger.error(
                f"No physical bucket locator found for region tag: {region_tag}"
            )
            return Response(
                status_code=500,
                content=f"No physical bucket locator found for upload region tag {region_tag}",
            )

        locators.append(
            DBPhysicalObjectLocator(
                logical_object=logical_object,
                location_tag=region_tag,
                cloud=physical_bucket_locator.cloud,
                region=physical_bucket_locator.region,
                bucket=physical_bucket_locator.bucket,
                key=physical_bucket_locator.prefix + request.key,
                lock_acquired_ts=datetime.utcnow(),
                status=Status.pending,
                is_primary=physical_bucket_locator.is_primary,
            )
        )

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


async def rm_lock_on_timeout(minutes, testing=False):
    # initial wait to prevent first check which should never run
    if not testing:
        await asyncio.sleep(minutes)
    while not stop_task_flag.is_set() or testing:
        async with engine.begin() as db:
            # db.begin()

            # calculate time for which we can timeout. Anything before or equal to 10 minutes ago will timeout
            cutoff_time = datetime.utcnow() - timedelta(minutes)

            # time out Physical objects that have been running for more than 10 minutes
            stmt_timeout_physical_objects = (
                update(DBPhysicalObjectLocator)
                .where(DBPhysicalObjectLocator.lock_acquired_ts <= cutoff_time)
                .values(status=Status.ready, lock_acquired_ts=None)
            )
            await db.execute(stmt_timeout_physical_objects)

            # time out Physical buckets that have been running for more than 10 minutes
            stmt_timeout_physical_buckets = (
                update(DBPhysicalBucketLocator)
                .where(DBPhysicalBucketLocator.lock_acquired_ts <= cutoff_time)
                .values(status=Status.ready, lock_acquired_ts=None)
            )
            await db.execute(stmt_timeout_physical_buckets)

            # find Logical objects that are pending
            stmt_find_pending_logical_objs = select(DBLogicalObject).where(
                DBLogicalObject.status == Status.pending
            )
            pendingLogicalObjs = (
                await db.execute(stmt_find_pending_logical_objs)
            ).fetchall()

            if pendingLogicalObjs is not None:
                # loop through list of pending logical objects
                for logical_obj in pendingLogicalObjs:
                    # get all physical objects corresponding to a given logical object
                    stmt3 = (
                        select(DBPhysicalObjectLocator)
                        .join(DBLogicalObject)
                        .where(
                            logical_obj.id == DBPhysicalObjectLocator.logical_object_id
                        )
                    )
                    objects = (await db.execute(stmt3)).fetchall()

                    # set logical objects status to "Ready" if all of its physical objects are "Ready"
                    if all([Status.ready == obj.status for obj in objects]):
                        edit_logical_obj_stmt = (
                            update(DBLogicalObject)
                            .where(objects[0].logical_object_id == logical_obj.id)
                            .values(status=Status.ready)
                        )
                        await db.execute(edit_logical_obj_stmt)

                # await db.commit()

            # find Logical buckets that are pending
            stmt_find_pending_logical_buckets = select(DBLogicalBucket).where(
                DBLogicalBucket.status == Status.pending
            )
            pendingLogicalBuckets = (
                await db.execute(stmt_find_pending_logical_buckets)
            ).fetchall()

            if pendingLogicalBuckets is not None:
                # loop through list of pending logical buckets
                for logical_bucket in pendingLogicalBuckets:
                    # get all physical buckets corresponding to a given logical object
                    stmt3 = (
                        select(DBPhysicalBucketLocator)
                        .join(DBLogicalBucket)
                        .where(
                            logical_bucket.id
                            == DBPhysicalBucketLocator.logical_bucket_id
                        )
                    )
                    buckets = (await db.execute(stmt3)).fetchall()

                    # set logical buckets status to "Ready" if all of its physical buckets are "Ready"
                    if all([Status.ready == bucket.status for bucket in buckets]):
                        edit_logical_bucket_stmt = (
                            update(DBLogicalBucket)
                            .where(buckets[0].logical_bucket_id == logical_bucket.id)
                            .values(status=Status.ready)
                        )
                        await db.execute(edit_logical_bucket_stmt)

            await db.commit()

        if testing:
            break

        await asyncio.sleep(minutes * 60)


@app.patch("/complete_upload")
async def complete_upload(request: PatchUploadIsCompleted, db: DBSession):
    stmt = (
        select(DBPhysicalObjectLocator)
        .join(DBLogicalObject)
        .options(joinedload(DBPhysicalObjectLocator.logical_object))
        .where(DBPhysicalObjectLocator.id == request.id)
    )
    physical_locator = await db.scalar(stmt)
    if physical_locator is None:
        logger.error(f"physical locator not found: {request}")
        return Response(status_code=404, content="Not Found")

    logger.debug(f"complete_upload: {request} -> {physical_locator}")

    physical_locator.status = Status.ready
    physical_locator.lock_acquired_ts = None
    if physical_locator.is_primary:
        # await db.refresh(physical_locator, ["logical_object"])
        logical_object = physical_locator.logical_object
        logical_object.status = Status.ready
        logical_object.size = request.size
        logical_object.etag = request.etag
        logical_object.last_modified = request.last_modified.replace(tzinfo=None)
    await db.commit()


@app.patch("/set_multipart_id")
async def set_multipart_id(request: PatchUploadMultipartUploadId, db: DBSession):
    stmt = (
        select(DBPhysicalObjectLocator)
        .join(DBLogicalObject)
        .where(DBPhysicalObjectLocator.id == request.id)
    )
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
    stmt = (
        select(DBPhysicalObjectLocator)
        .join(DBLogicalObject)
        .where(DBPhysicalObjectLocator.id == request.id)
    )
    physical_locator = await db.scalar(stmt)
    if physical_locator is None:
        logger.error(f"physical locator not found: {request}")
        return Response(status_code=404, content="Not Found")
    await db.refresh(physical_locator, ["logical_object"])

    logger.debug(f"append_part: {request} -> {physical_locator}")

    await db.refresh(physical_locator, ["multipart_upload_parts"])

    existing_physical_part = next(
        (
            part
            for part in physical_locator.multipart_upload_parts
            if part.part_number == request.part_number
        ),
        None,
    )

    if existing_physical_part:
        existing_physical_part.etag = request.etag
        existing_physical_part.size = request.size
    else:
        physical_locator.multipart_upload_parts.append(
            DBPhysicalMultipartUploadPart(
                part_number=request.part_number,
                etag=request.etag,
                size=request.size,
            )
        )

    if physical_locator.is_primary:
        await db.refresh(physical_locator.logical_object, ["multipart_upload_parts"])
        existing_logical_part = next(
            (
                part
                for part in physical_locator.logical_object.multipart_upload_parts
                if part.part_number == request.part_number
            ),
            None,
        )

        if existing_logical_part:
            existing_logical_part.etag = request.etag
            existing_logical_part.size = request.size
        else:
            physical_locator.logical_object.multipart_upload_parts.append(
                DBLogicalMultipartUploadPart(
                    part_number=request.part_number,
                    etag=request.etag,
                    size=request.size,
                )
            )

    await db.commit()


@app.post("/continue_upload")
async def continue_upload(
    request: ContinueUploadRequest, db: DBSession
) -> List[ContinueUploadResponse]:
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
            status=locator.status,
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
            copy_src_bucket=copy_src_buckets[i]
            if request.copy_src_bucket is not None
            else None,
            copy_src_key=copy_src_keys[i] if request.copy_src_key is not None else None,
        )
        for i, locator in enumerate(locators)
    ]


@app.post("/list_buckets")
async def list_buckets(db: DBSession) -> List[BucketResponse]:
    stmt = select(DBLogicalBucket).where(DBLogicalBucket.status == Status.ready)
    buckets = (await db.scalars(stmt)).all()

    logger.debug(f"list_buckets: -> {buckets}")

    return [
        BucketResponse(
            bucket=bucket.bucket,
            creation_date=bucket.creation_date,
        )
        for bucket in buckets
    ]


@app.post("/list_objects")
async def list_objects(
    request: ListObjectRequest, db: DBSession
) -> List[ObjectResponse]:
    stmt = select(DBLogicalBucket).where(
        DBLogicalBucket.bucket == request.bucket, DBLogicalBucket.status == Status.ready
    )
    logical_bucket = await db.scalar(stmt)
    if logical_bucket is None:
        return Response(status_code=404, content="Bucket Not Found")

    stmt = select(DBLogicalObject).where(
        DBLogicalObject.bucket == logical_bucket.bucket,
        DBLogicalObject.status == Status.ready,
    )
    if request.prefix is not None:
        stmt = stmt.where(DBLogicalObject.key.startswith(request.prefix))
    if request.start_after is not None:
        stmt = stmt.where(DBLogicalObject.key > request.start_after)

    # Sort keys before return
    stmt = stmt.order_by(DBLogicalObject.key)

    # Limit the number of returned objects if specified
    if request.max_keys is not None:
        stmt = stmt.limit(request.max_keys)

    objects = await db.execute(stmt)
    objects_all = objects.scalars().all()

    if not objects_all:
        return []

    logger.debug(f"list_objects: {request} -> {objects_all}")

    return [
        ObjectResponse(
            bucket=obj.bucket,
            key=obj.key,
            size=obj.size,
            etag=obj.etag,
            last_modified=obj.last_modified,
        )
        for obj in objects_all
    ]


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


@app.post("/list_multipart_uploads")
async def list_multipart_uploads(
    request: ListObjectRequest, db: DBSession
) -> List[MultipartResponse]:
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


@app.post("/list_parts")
async def list_parts(
    request: ListPartsRequest, db: DBSession
) -> List[LogicalPartResponse]:
    stmt = (
        select(DBLogicalObject)
        .where(DBLogicalObject.bucket == request.bucket)
        .where(DBLogicalObject.key == request.key)
        .where(DBLogicalObject.status == Status.pending)
        .where(DBLogicalObject.multipart_upload_id == request.upload_id)
    )
    objects = (await db.scalars(stmt)).all()
    if len(objects) == 0:
        return Response(status_code=404, content="Object Multipart Not Found")

    assert len(objects) == 1, "should only have one object"
    await db.refresh(objects[0], ["multipart_upload_parts"])

    logger.debug(
        f"list_parts: {request} -> {objects[0], objects[0].multipart_upload_parts}"
    )

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


@app.post(
    "/locate_object_status",
    responses={
        status.HTTP_200_OK: {"model": ObjectStatus},
        status.HTTP_404_NOT_FOUND: {"description": "Object not found"},
    },
)
async def locate_object_status(
    request: LocateObjectRequest, db: DBSession
) -> ObjectStatus:
    """Given the logical object information, return the status of the object. Currently only used for testing metadata cleanup."""
    stmt = (
        select(DBPhysicalObjectLocator)
        .join(DBLogicalObject)
        .where(DBLogicalObject.bucket == request.bucket)
        .where(DBLogicalObject.key == request.key)
    )
    # get physical locators
    locators = (await db.scalars(stmt)).all()
    if len(locators) == 0:
        return Response(status_code=404, content="Object Not Found")

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

    logger.debug(
        f"locate_object: chosen locator with strategy {reason} out of {len(locators)}, {request} -> {chosen_locator}"
    )

    await db.refresh(chosen_locator, ["logical_object"])
    # return object status
    return ObjectStatus(status=chosen_locator.status)


@app.post(
    "/locate_bucket_status",
    responses={
        status.HTTP_200_OK: {"model": BucketStatus},
        status.HTTP_404_NOT_FOUND: {"description": "Bucket not found"},
    },
)
async def locate_bucket_status(
    request: LocateBucketRequest, db: DBSession
) -> BucketStatus:
    """Given the bucket name, return physical bucket status. Currently only used for testing metadata cleanup"""
    stmt = (
        select(DBPhysicalBucketLocator)
        .join(DBLogicalBucket)
        .where(DBLogicalBucket.bucket == request.bucket)
    )
    # get buckets corresponding to data from request
    locators = (await db.scalars(stmt)).all()

    if len(locators) == 0:
        return Response(status_code=404, content="Bucket Not Found")

    chosen_locator = None
    for locator in locators:
        if locator.location_tag == request.client_from_region:
            chosen_locator = locator
            break
    else:
        # find the primary locator
        chosen_locator = next(locator for locator in locators if locator.is_primary)

    await db.refresh(chosen_locator, ["logical_bucket"])

    # return status
    return BucketStatus(
        status=chosen_locator.status,
    )


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
