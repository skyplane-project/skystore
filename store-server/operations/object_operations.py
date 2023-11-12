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
from operations.schemas.bucket_schemas import DBLogicalBucket
from sqlalchemy.orm import selectinload, Session
from itertools import zip_longest
from sqlalchemy import select
from sqlalchemy import and_
from sqlalchemy import func
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
                .options(selectinload(DBLogicalObject.physical_object_locators))
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
                .options(selectinload(DBLogicalObject.physical_object_locators))
                .where(DBLogicalObject.bucket == request.bucket)
                .where(DBLogicalObject.key == key)
                .where(DBLogicalObject.status == Status.ready)
            )
        # multiple versioning support
        logical_objs = (await db.scalars(stmt)).all()
        stmt1 = (
            select(DBLogicalObject)
            .where(DBLogicalObject.bucket == request.bucket)
            .where(DBLogicalObject.key == key)
            .where(DBLogicalObject.status == Status.ready)
        )
        print("logical_objs: ", logical_objs)
        print("All logical objs with this key: ", (await db.scalars(stmt1)).all())
        if len(logical_objs) == 0:
            return Response(status_code=404, content="Objects not found")

        locators = []
        for logical_obj in logical_objs:
            for physical_locator in logical_obj.physical_object_locators:
                # if the set is empty, delete all the versions
                if (
                    len(request.object_identifiers[key]) > 0
                    and physical_locator.version_id
                    not in request.object_identifiers[key]
                ):
                    continue  # skip if the version_id is not in the request

                if (
                    physical_locator.status not in Status.ready
                    and not multipart_upload_id
                ):
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
                        version_id=physical_locator.version_id,
                    )
                )

            logical_obj.status = Status.pending_deletion

            # TODO: should we commit here or after deleteing all the logical objects versions?
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

        # only delete the logical object with same version if there is no other physical object locator if possible
        remaining_physical_locators_stmt = (
            select(DBPhysicalObjectLocator)
            .where(
                DBPhysicalObjectLocator.logical_object_id
                == physical_locator.logical_object.id
            )
            .where(
                DBPhysicalObjectLocator.logical_object_version
                == physical_locator.logical_object.version
            )
        )
        remaining_physical_locators = await db.execute(remaining_physical_locators_stmt)
        if not remaining_physical_locators.all():
            await db.delete(physical_locator.logical_object)

        try:
            await db.commit()
        except Exception as e:
            logger.error(f"Error occurred while committing changes: {e}")
            return Response(status_code=500, content="Error committing changes")


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
        .join(
            DBLogicalObject,
            and_(
                DBLogicalObject.id == DBPhysicalObjectLocator.logical_object_id,
                DBLogicalObject.version
                == DBPhysicalObjectLocator.logical_object_version,
            ),
        )
        .where(DBLogicalObject.bucket == request.bucket)
        .where(DBLogicalObject.key == request.key)
        .where(DBLogicalObject.status == Status.ready)
        .where(DBPhysicalObjectLocator.status == Status.ready)
    )
    locators = (await db.scalars(stmt)).all()

    if len(locators) == 0:
        return Response(status_code=404, content="Object Not Found")

    chosen_locator = None
    newest_version = 0
    reason = ""

    # if request.get_primary:
    #     chosen_locator = next(locator for locator in locators if locator.is_primary)
    #     reason = "exact match (primary)"
    # else:

    if request.version_id is not None:
        # find the locator with the exact version
        for locator in locators:
            if locator.version_id == request.version_id:
                chosen_locator = locator
                reason = "exact match"
                break
    else:
        for locator in locators:
            # always read the lastest version
            if (
                locator.location_tag == request.client_from_region
                and locator.version >= newest_version
            ):
                chosen_locator = locator
                newest_version = locator.version
                reason = "exact match"
                break

        if chosen_locator is None:
            # find the primary locator
            for locator in locators:
                # always read the lastest version
                if locator.is_primary and locator.version >= newest_version:
                    chosen_locator = locator
                    newest_version = locator.version
                    reason = "primary"
                    break

    print("chosen locator: ", chosen_locator)

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
        version_id=chosen_locator.version_id,
    )


@router.post("/start_warmup")
async def start_warmup(
    request: StartWarmupRequest, db: Session = Depends(get_session)
) -> StartWarmupResponse:
    """Given the logical object information and warmup regions, return one or zero physical object locators."""
    stmt = (
        select(DBPhysicalObjectLocator)
        .options(selectinload(DBPhysicalObjectLocator.logical_object))
        .join(DBLogicalObject)
        .where(DBLogicalObject.bucket == request.bucket)
        .where(DBLogicalObject.key == request.key)
        .where(DBLogicalObject.status == Status.ready)
    )
    locators = (await db.scalars(stmt)).all()

    if not locators:
        return Response(status_code=404, content="Object Not Found")

    newest_version = 0

    # TODO: Transfer from primary locator, select the most recent logical object version
    for locator in locators:
        if locator.is_primary and locator.version >= newest_version:
            primary_locator = locator
            newest_version = locator.version
            break

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
            version_id=primary_locator.version_id,  # same version of the primary locator
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
            version_id=primary_locator.version_id,
        ),
        dst_locators=[
            LocateObjectResponse(
                id=locator.id,
                tag=locator.location_tag,
                cloud=locator.cloud,
                bucket=locator.bucket,
                region=locator.region,
                key=locator.key,
                version_id=locator.version_id,
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
        .join(
            DBLogicalObject,
            and_(
                DBLogicalObject.id == DBPhysicalObjectLocator.logical_object_id,
                DBLogicalObject.version
                == DBPhysicalObjectLocator.logical_object_version,
            ),
        )
        .where(DBLogicalObject.bucket == request.bucket)
        .where(DBLogicalObject.key == request.key)
        .where(DBLogicalObject.status == Status.ready)
    )
    existing_objects = (await db.scalars(existing_objects_stmt)).all()
    # Parse results for the object_already_exists check
    # object_already_exists = any(
    #     locator.location_tag == request.client_from_region
    #     for locator in existing_objects
    # )

    # should not do this when versioning is enabled
    # if object_already_exists:
    #     logger.error("This exact object already exists")
    #     return Response(status_code=409, content="Conflict, object already exists")

    existing_tags = set(locator.location_tag for locator in existing_objects)
    primary_exists = any(locator.is_primary for locator in existing_objects)

    # version_id is None: should copy the latest version of the object
    # version_id is not None, should copy the corresponding version
    if (request.copy_src_bucket is not None) and (request.copy_src_key is not None):
        copy_src_stmt = (
            select(DBPhysicalObjectLocator)
            .join(
                DBLogicalObject,
                and_(
                    DBLogicalObject.id == DBPhysicalObjectLocator.logical_object_id,
                    DBLogicalObject.version
                    == DBPhysicalObjectLocator.logical_object_version,
                ),
            )
            .where(DBLogicalObject.bucket == request.copy_src_bucket)
            .where(DBLogicalObject.key == request.copy_src_key)
            .where(DBLogicalObject.status == Status.ready)
        )

        if request.version_id is not None:
            copy_src_stmt = copy_src_stmt.where(
                DBPhysicalObjectLocator.version_id == request.version_id
            )

        copy_src_locators = (await db.scalars(copy_src_stmt)).all()

        copy_src_locators_map = {}  # location_tag -> copy_src_locator

        if request.version_id is not None:
            # assert len(copy_src_locators) > 0, "should have this copy_src_locator"
            copy_src_locators_map = {
                locator.location_tag: locator for locator in copy_src_locators
            }
        else:  # copy the latest version of the object
            for src_locator in copy_src_locators:
                tag = src_locator.location_tag
                if tag not in copy_src_locators_map or (
                    tag in copy_src_locators_map
                    and copy_src_locators_map[tag].version < src_locator.version
                ):
                    copy_src_locators_map[tag] = src_locator

        # copy_src_locators_map = {
        #     locator.location_tag: locator for locator in copy_src_locators
        # }
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
        # fetach the latest version of the object
        if request.version_id is None:
            newest_version = 0
            for existing_object in existing_objects:
                await db.refresh(existing_object, ["logical_object"])
                if existing_object.version >= newest_version:
                    newest_version = existing_object.version
                    # select the newest version of the object
                    logical_object = existing_object.logical_object
        else:
            # fetch the specific version
            for existing_object in existing_objects:
                await db.refresh(existing_object, ["logical_object"])
                if existing_object.version_id == request.version_id:
                    logical_object = existing_object.logical_object
                    break

        # if it is copy-on-read, no need to create new logical object
        if request.policy != "copy_on_read":
            # version support: create a new logical object based on the previous logical object fields (the previous newest version)
            logical_object = DBLogicalObject(
                bucket=logical_object.bucket,
                key=logical_object.key,
                size=logical_object.size,
                last_modified=logical_object.last_modified,  # this field should be the current timestamp, here we can just ignore since this will be updated in the complete upload step
                etag=logical_object.etag,
                status=Status.pending,
                multipart_upload_id=logical_object.multipart_upload_id,
                version=logical_object.version
                + 1,  # increment version_id for each logical object
            )
            db.add(logical_object)

    logical_bucket = (
        await db.execute(
            select(DBLogicalBucket)
            .options(selectinload(DBLogicalBucket.physical_bucket_locators))
            .where(DBLogicalBucket.bucket == request.bucket)
        )
    ).scalar_one_or_none()
    physical_bucket_locators = logical_bucket.physical_bucket_locators

    primary_write_region = None

    if primary_exists:
        # Assume that physical bucket locators for this region already exists and we don't need to create them
        # For pull-on-read
        upload_to_region_tags = [request.client_from_region]
        primary_write_region = [
            locator.location_tag for locator in existing_objects if locator.is_primary
        ]
        # assert (
        #     len(primary_write_region) == 1
        # ), "should only have one primary write region"

        # but we can choose wheatever idx in the primary_write_region list
        primary_write_region = primary_write_region[0]
        assert (
            primary_write_region != request.client_from_region
        ), "should not be the same region"
    else:
        # NOTE: Push-based: upload to primary region and broadcast to other regions marked with need_warmup
        if request.policy == "push":
            # Except this case, always set the first-write region of the OBJECT to be primary
            upload_to_region_tags = [
                locator.location_tag
                for locator in physical_bucket_locators
                if locator.is_primary or locator.need_warmup
            ]
            primary_write_region = [
                locator.location_tag
                for locator in physical_bucket_locators
                if locator.is_primary
            ]
            # assert (
            #     len(primary_write_region) == 1
            # ), "should only have one primary write region"
            primary_write_region = primary_write_region[0]
        else:
            # Write to the local region and set the first-write region of the OBJECT to be primary
            upload_to_region_tags = [request.client_from_region]
            primary_write_region = request.client_from_region

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
                logical_object=logical_object,  # link the physical object with the new logical object
                location_tag=region_tag,
                cloud=physical_bucket_locator.cloud,
                region=physical_bucket_locator.region,
                bucket=physical_bucket_locator.bucket,
                key=physical_bucket_locator.prefix + request.key,
                lock_acquired_ts=datetime.utcnow(),
                status=Status.pending,
                is_primary=(
                    region_tag == primary_write_region
                ),  # NOTE: location of first write is primary
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
                # version_id=locator.version_id,
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
        .join(
            DBLogicalObject,
            and_(
                DBLogicalObject.id == DBPhysicalObjectLocator.logical_object_id,
                DBLogicalObject.version
                == DBPhysicalObjectLocator.logical_object_version,
            ),
        )
        .options(selectinload(DBPhysicalObjectLocator.logical_object))
        .where(DBPhysicalObjectLocator.id == request.id)
    )
    physical_locator = await db.scalar(stmt)
    if physical_locator is None:
        logger.error(f"physical locator not found: {request}")
        return Response(status_code=404, content="Not Found")

    logger.debug(f"complete_upload: {request} -> {physical_locator}")

    physical_locator.status = Status.ready
    physical_locator.lock_acquired_ts = None
    physical_locator.version_id = request.version_id

    # TODO: might need to change the if conditions for different policies
    if (
        (request.policy == "push" and physical_locator.is_primary)
        or request.policy == "write_local"
        or request.policy == "copy_on_read"
    ):
        # NOTE: might not need to update the logical object for consecutive reads for copy_on_read
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
        .join(
            DBLogicalObject,
            and_(
                DBLogicalObject.id == DBPhysicalObjectLocator.logical_object_id,
                DBLogicalObject.version
                == DBPhysicalObjectLocator.logical_object_version,
            ),
        )
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
        .join(
            DBLogicalObject,
            and_(
                DBLogicalObject.id == DBPhysicalObjectLocator.logical_object_id,
                DBLogicalObject.version
                == DBPhysicalObjectLocator.logical_object_version,
            ),
        )
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
        .join(
            DBLogicalObject,
            and_(
                DBLogicalObject.id == DBPhysicalObjectLocator.logical_object_id,
                DBLogicalObject.version
                == DBPhysicalObjectLocator.logical_object_version,
            ),
        )
        .where(DBLogicalObject.bucket == request.bucket)
        .where(DBLogicalObject.key == request.key)
        .where(DBLogicalObject.status == Status.pending)
        .where(DBLogicalObject.multipart_upload_id == request.multipart_upload_id)
    )
    locators = (await db.scalars(stmt)).all()
    if len(locators) == 0:
        return Response(status_code=404, content="Not Found")
    locators_dict = {}
    for locator in locators:
        # only save the ones with newest versions:
        if locator.location_tag not in locators_dict or (
            locator.location_tag in locators_dict
            and locators_dict[locator.location_tag].version < locator.version
        ):
            locators_dict[locator.location_tag] = locator
    locators = list(locators_dict.values())
    copy_src_buckets, copy_src_keys = [], []
    if request.copy_src_bucket is not None and request.copy_src_key is not None:
        physical_src_locators = (
            await db.scalars(
                select(DBPhysicalObjectLocator)
                .join(
                    DBLogicalObject,
                    and_(
                        DBLogicalObject.id == DBPhysicalObjectLocator.logical_object_id,
                        DBLogicalObject.version
                        == DBPhysicalObjectLocator.logical_object_version,
                    ),
                )
                .where(DBLogicalObject.bucket == request.copy_src_bucket)
                .where(DBLogicalObject.key == request.copy_src_key)
                .where(DBLogicalObject.status == Status.ready)
            )
        ).all()
        # only save the ones with newest versions:
        physical_src_locators_dict = {}
        for locator in physical_src_locators:
            if locator.location_tag not in physical_src_locators_dict or (
                locator.location_tag in physical_src_locators_dict
                and physical_src_locators_dict[locator.location_tag].version
                < locator.version
            ):
                physical_src_locators_dict[locator.location_tag] = locator
        physical_src_locators = list(physical_src_locators_dict.values())
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

    stmt = select(
        [
            DBLogicalObject.id,
            DBLogicalObject.bucket,
            DBLogicalObject.key,
            DBLogicalObject.size,
            DBLogicalObject.etag,
            DBLogicalObject.last_modified,
            DBLogicalObject.status,
            DBLogicalObject.multipart_upload_id,
            func.max(DBLogicalObject.version),
        ]
    ).where(
        DBLogicalObject.bucket == logical_bucket.bucket,
        DBLogicalObject.status == Status.ready,
    )
    if request.prefix is not None:
        stmt = stmt.where(DBLogicalObject.key.startswith(request.prefix))
    if request.start_after is not None:
        stmt = stmt.where(DBLogicalObject.key > request.start_after)

    # Sort keys before return
    stmt = stmt.group_by(DBLogicalObject.key).order_by(DBLogicalObject.key)

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
    # TODO: should only head the latest version of the object?
    obj_lst = (await db.scalars(stmt)).all()

    if len(obj_lst) == 0:
        return Response(status_code=404, content="Not Found")

    logical_object = None

    # if the version id is not given, find the latest version of the object
    # otherwise, find the logical object linked with the given physical version id
    if request.version_id is not None:
        for logical_obj in obj_lst:
            for physical_obj in logical_obj.physical_object_locators:
                await db.refresh(physical_obj, ["physical_object_locators"])
                if physical_obj.version_id == request.version_id:
                    logical_object = logical_obj
                    break
            if logical_object is not None:
                break

        if logical_object is None:
            return Response(
                status_code=404,
                content="Not Found the version {}".format(request.version_id),
            )
    else:
        newest_version = 0
        # find the latest version of the object
        for object in obj_lst:
            if object.version >= newest_version:
                logical_object = object
                newest_version = object.version

    logger.debug(f"head_object: {request} -> {logical_object}")

    return HeadObjectResponse(
        bucket=logical_object.bucket,
        key=logical_object.key,
        size=logical_object.size,
        etag=logical_object.etag,
        last_modified=logical_object.last_modified,
        version_id=request.version_id,
    )


# TODO: Can multipart upload upload an object with the same key of some other object? (I don't think so)
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

    # only keep the latest version of the object
    objects_dict = {}
    for obj in objects:
        if obj.key not in objects_dict or (
            obj.key in objects_dict and objects_dict[obj.key].version < obj.version
        ):
            objects_dict[obj.key] = obj
    objects = list(objects_dict.values())

    logger.debug(f"list_multipart_uploads: {request} -> {objects}")

    return [
        MultipartResponse(
            bucket=obj.bucket,
            key=obj.key,
            upload_id=obj.multipart_upload_id,
        )
        for obj in objects
    ]


# TODO: consider only the latest version of the object?
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

    for object in objects:
        await db.refresh(object, ["multipart_upload_parts"])

    # only keep the latest version of the object
    # TODO: change to sort method of list
    objects_dict = {}
    for obj in objects:
        if obj.key not in objects_dict or (
            obj.key in objects_dict and objects_dict[obj.key].version < obj.version
        ):
            objects_dict[obj.key] = obj
    objects = list(objects_dict.values())

    assert len(objects) == 1, "should only have one object"

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


# TODO: I think we need to return a list
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
        .join(
            DBLogicalObject,
            and_(
                DBLogicalObject.id == DBPhysicalObjectLocator.logical_object_id,
                DBLogicalObject.version
                == DBPhysicalObjectLocator.logical_object_version,
            ),
        )
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

    if chosen_locator is None:
        # find the primary locator
        chosen_locator = next(locator for locator in locators if locator.is_primary)
        reason = "fallback to primary"

    logger.debug(
        f"locate_object: chosen locator with strategy {reason} out of {len(locators)}, {request} -> {chosen_locator}"
    )

    await db.refresh(chosen_locator, ["logical_object"])
    # return object status
    return ObjectStatus(status=chosen_locator.status)
