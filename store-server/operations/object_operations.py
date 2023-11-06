import uuid
from operations.schemas.object_schemas import (
    DBLogicalObject,
    DBPhysicalObjectLocator,
    DBLogicalMultipartUploadPart,
    DBPhysicalMultipartUploadPart,
    ObjectResponse,
    LocateObjectRequest,
    LocateObjectResponse,
    DeleteObjectsRequest,
    DeleteObjectsResponse,
    DeleteObjectsIsCompleted,
    ObjectStatus,
    StartUploadRequest,
    StartWarmupRequest,
    StartWarmupResponse,
    StartUploadResponse,
    PatchUploadIsCompleted,
    PatchUploadMultipartUploadId,
    PatchUploadMultipartUploadPart,
    ContinueUploadRequest,
    ContinueUploadResponse,
    ContinueUploadPhysicalPart,
    ListObjectRequest,
    HeadObjectRequest,
    HeadObjectResponse,
    ListPartsRequest,
    LogicalPartResponse,
    MultipartResponse,
)
from operations.schemas.bucket_schemas import DBLogicalBucket, HeadBucketRequest
from sqlalchemy.orm import selectinload, joinedload, Session
from itertools import zip_longest
from sqlalchemy import select
from operations.utils.conf import Status
from fastapi import APIRouter, Response, Depends, status
from operations.utils.db import get_session, logger
from typing import List
from datetime import datetime

router = APIRouter()


@router.post("/start_delete_objects")
async def start_delete_objects(
    request: DeleteObjectsRequest, db: Session = Depends(get_session)
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


@router.patch("/complete_delete_objects")
async def complete_delete_objects(
    request: DeleteObjectsIsCompleted, db: Session = Depends(get_session)
):
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


@router.post(
    "/locate_object",
    responses={
        status.HTTP_200_OK: {"model": LocateObjectResponse},
        status.HTTP_404_NOT_FOUND: {"description": "Object not found"},
    },
)
async def locate_object(
    request: LocateObjectRequest, db: Session = Depends(get_session)
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
    else:
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


@router.post("/start_warmup")
async def start_warmup(
    request: StartWarmupRequest, db: Session = Depends(get_session)
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


@router.post("/start_upload")
async def start_upload(
    request: StartUploadRequest, db: Session = Depends(get_session)
) -> StartUploadResponse:
    # TODO: policy check

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
        # TODO: I think this is the pull-on-read scenario?
        upload_to_region_tags = [request.client_from_region]
    else:
        # NOTE: Push-based: upload to primary region and broadcast to other regions marked with need_warmup
        if request.policy == "push":
            upload_to_region_tags = [
                locator.location_tag
                for locator in physical_bucket_locators
                if locator.is_primary or locator.need_warmup
            ]
        else:
            upload_to_region_tags = [request.client_from_region]

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


@router.patch("/complete_upload")
async def complete_upload(
    request: PatchUploadIsCompleted, db: Session = Depends(get_session)
):
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

    if (
        request.policy == "push" and physical_locator.is_primary
    ) or request.policy == "write-local":
        # await db.refresh(physical_locator, ["logical_object"])
        logical_object = physical_locator.logical_object
        logical_object.status = Status.ready
        logical_object.size = request.size
        logical_object.etag = request.etag
        logical_object.last_modified = request.last_modified.replace(tzinfo=None)
    await db.commit()


@router.patch("/set_multipart_id")
async def set_multipart_id(
    request: PatchUploadMultipartUploadId, db: Session = Depends(get_session)
):
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


@router.patch("/append_part")
async def append_part(
    request: PatchUploadMultipartUploadPart, db: Session = Depends(get_session)
):
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


@router.post("/continue_upload")
async def continue_upload(
    request: ContinueUploadRequest, db: Session = Depends(get_session)
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


@router.post("/list_objects")
async def list_objects(
    request: ListObjectRequest, db: Session = Depends(get_session)
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


@router.post("/head_object")
async def head_object(
    request: HeadObjectRequest, db: Session = Depends(get_session)
) -> HeadObjectResponse:
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


@router.post("/list_multipart_uploads")
async def list_multipart_uploads(
    request: ListObjectRequest, db: Session = Depends(get_session)
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


@router.post("/list_parts")
async def list_parts(
    request: ListPartsRequest, db: Session = Depends(get_session)
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


@router.post(
    "/locate_object_status",
    responses={
        status.HTTP_200_OK: {"model": ObjectStatus},
        status.HTTP_404_NOT_FOUND: {"description": "Object not found"},
    },
)
async def locate_object_status(
    request: LocateObjectRequest, db: Session = Depends(get_session)
) -> ObjectStatus:
    """Given the logical object information, return the status of the object.
    Currently only used for testing metadata cleanup."""

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
