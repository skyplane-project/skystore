from operations.schemas.bucket_schemas import (
    DBLogicalBucket,
    DBPhysicalBucketLocator,
    BucketResponse,
    RegisterBucketRequest,
    CreateBucketRequest,
    CreateBucketResponse,
    CreateBucketIsCompleted,
    DeleteBucketRequest,
    DeleteBucketResponse,
    DeleteBucketIsCompleted,
    LocateBucketRequest,
    LocateBucketResponse,
    HeadBucketRequest,
    BucketStatus,
    PutBucketVersioningRequest,
)
from datetime import datetime
from sqlalchemy.orm import joinedload
from fastapi import Response
from sqlalchemy import select
from operations.utils.conf import (
    Status,
    DEFAULT_INIT_REGIONS,
    DEFAULT_SKYSTORE_BUCKET_PREFIX,
)
from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends, status
from operations.utils.db import get_session, logger
from typing import List
import os

router = APIRouter()
init_region_tags = (
    os.getenv("INIT_REGIONS").split(",")
    if os.getenv("INIT_REGIONS")
    else DEFAULT_INIT_REGIONS
)
skystore_bucket_prefix = (
    os.getenv("SKYSTORE_BUCKET_PREFIX")
    if os.getenv("SKYSTORE_BUCKET_PREFIX")
    else DEFAULT_SKYSTORE_BUCKET_PREFIX
)


@router.post("/register_buckets")
async def register_buckets(
    request: RegisterBucketRequest, db: Session = Depends(get_session)
) -> Response:
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
    # TODO: remove the logic of physical bucket primary region, set it as object-level for now
    # Look at start_upload function
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


@router.post("/start_create_bucket")
async def start_create_bucket(
    request: CreateBucketRequest, db: Session = Depends(get_session)
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


@router.patch("/complete_create_bucket")
async def complete_create_bucket(
    request: CreateBucketIsCompleted, db: Session = Depends(get_session)
):
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


@router.post("/start_delete_bucket")
async def start_delete_bucket(
    request: DeleteBucketRequest, db: Session = Depends(get_session)
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


@router.patch("/complete_delete_bucket")
async def complete_delete_bucket(
    request: DeleteBucketIsCompleted, db: Session = Depends(get_session)
):
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


@router.post(
    "/locate_bucket",
    responses={
        status.HTTP_200_OK: {"model": LocateBucketResponse},
        status.HTTP_404_NOT_FOUND: {"description": "Bucket not found"},
    },
)
async def locate_bucket(
    request: LocateBucketRequest, db: Session = Depends(get_session)
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


@router.post("/list_buckets")
async def list_buckets(db: Session = Depends(get_session)) -> List[BucketResponse]:
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


@router.post("/head_bucket")
async def head_bucket(request: HeadBucketRequest, db: Session = Depends(get_session)):
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


@router.post("/put_bucket_versioning")
async def put_bucket_versioning(
    request: PutBucketVersioningRequest, db: Session = Depends(get_session)
) -> List[LocateBucketResponse]:
    stmt = select(DBLogicalBucket).where(
        DBLogicalBucket.bucket == request.bucket, DBLogicalBucket.status == Status.ready
    )
    bucket = await db.scalar(stmt)

    if bucket is None:
        return Response(status_code=404, content="Not Found")

    logger.debug(f"put_bucket_versioning: {request} -> {bucket}")

    bucket.version_enabled = request.versioning

    # besides changing the logical bucket versioning setting, we should be able to let the
    # proxy side change the corresponding physical bucket versioning setting
    locators_lst = []
    await db.refresh(bucket, ["physical_bucket_locators"])
    for physical_bucket_locator in bucket.physical_bucket_locators:
        locators_lst.append(
            LocateBucketResponse(
                id=physical_bucket_locator.id,
                tag=physical_bucket_locator.location_tag,
                cloud=physical_bucket_locator.cloud,
                bucket=physical_bucket_locator.bucket,
                region=physical_bucket_locator.region,
            )
        )

    await db.commit()

    return locators_lst


@router.post("/check_version_setting")
async def check_version_setting(
    request: HeadBucketRequest, db: Session = Depends(get_session)
) -> bool:
    stmt = select(DBLogicalBucket).where(
        DBLogicalBucket.bucket == request.bucket, DBLogicalBucket.status == Status.ready
    )
    bucket = await db.scalar(stmt)

    if bucket is None:
        return Response(status_code=404, content="Not Found")

    logger.debug(f"check_version_setting: {request} -> {bucket}")

    # both suspended and enabled versioning setting should be able to upload objects multiple times
    if bucket.version_enabled is None:
        return False
    return True


@router.post(
    "/locate_bucket_status",
    responses={
        status.HTTP_200_OK: {"model": BucketStatus},
        status.HTTP_404_NOT_FOUND: {"description": "Bucket not found"},
    },
)
async def locate_bucket_status(
    request: LocateBucketRequest, db: Session = Depends(get_session)
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
