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
from sqlalchemy.orm import selectinload, Session, joinedload
from itertools import zip_longest
from sqlalchemy.sql import select
from sqlalchemy import and_
from sqlalchemy import func
from sqlalchemy import text
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

    if request.multipart_upload_ids and len(request.object_identifiers) != len(
        request.multipart_upload_ids
    ):
        return Response(
            status_code=400,
            content="Mismatched lengths for ids and multipart_upload_ids",
        )

    locator_dict = {}
    for key, multipart_upload_id in zip_longest(
        request.object_identifiers, request.multipart_upload_ids or []
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
            print("key: ", key)
            stmt = (
                select(DBLogicalObject)
                .options(selectinload(DBLogicalObject.physical_object_locators))
                .where(DBLogicalObject.bucket == request.bucket)
                .where(DBLogicalObject.key == key)
                .where(DBLogicalObject.status == Status.ready)
            )
        # multiple versioning support
        logical_objs = (await db.scalars(stmt)).all()
        print("logical_objs: ", logical_objs)
        # stmt1 = (
        #     select(DBLogicalObject)
        #     .where(DBLogicalObject.bucket == request.bucket)
        #     .where(DBLogicalObject.key == key)
        #     .where(DBLogicalObject.status == Status.ready)
        # )
        # print("logical_objs: ", logical_objs)
        # print("All logical objs with this key: ", (await db.scalars(stmt1)).all())
        if len(logical_objs) == 0:
            return Response(status_code=404, content="Objects not found")

        locators = []
        for logical_obj in logical_objs:
            # if the set is empty, delete all the versions
            if (
                len(request.object_identifiers[key]) > 0 
                and (logical_obj.id
                not in request.object_identifiers[key])
            ):
                continue  # skip if the version_id is not in the request

            for physical_locator in logical_obj.physical_object_locators:
                await db.refresh(physical_locator, ["logical_object"])

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
                        version_id=physical_locator.version_id, # this is already an optional string, might be None.
                        version=physical_locator.logical_object.id
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

        # only delete the logical object with same version if there is no other physical object locator if possible
        remaining_physical_locators_stmt = (
            select(DBPhysicalObjectLocator)
            .where(
                DBPhysicalObjectLocator.logical_object_id
                == physical_locator.logical_object.id
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

    if request.version_id is not None:
        stmt = (
            select(DBLogicalObject)
            .join(DBPhysicalObjectLocator)
            .where(DBLogicalObject.bucket == request.bucket)
            .where(DBLogicalObject.key == request.key)
            .where(DBLogicalObject.status == Status.ready)
            .where(DBLogicalObject.id == request.version_id)    # select the one with specific version
            .where(DBPhysicalObjectLocator.status == Status.ready)
        )
    else:
        stmt = (
            select(DBLogicalObject)
            .join(DBPhysicalObjectLocator)
            .where(DBLogicalObject.bucket == request.bucket)
            .where(DBLogicalObject.key == request.key)
            .where(DBLogicalObject.status == Status.ready)
            .where(DBPhysicalObjectLocator.status == Status.ready)
            .order_by(DBLogicalObject.id.desc())    # select the latest version
            #.first()
        )
    locators = (await db.scalars(stmt)).first()
    print("locate_object locators: ", locators)
    if locators is None:
        return Response(status_code=404, content="Object Not Found")

    chosen_locator = None
    reason = ""

    # if request.get_primary:
    #     chosen_locator = next(locator for locator in locators if locator.is_primary)
    #     reason = "exact match (primary)"
    # else:
    await db.refresh(locators, ["physical_object_locators"])
    for physical_locator in locators.physical_object_locators:
        if (
            physical_locator.location_tag == request.client_from_region
        ):
            chosen_locator = physical_locator
            reason = "exact match"
            break

    if chosen_locator is None:
        # find the primary locator
        for physical_locator in locators.physical_object_locators:
            # always read the lastest version
            if physical_locator.is_primary:
                chosen_locator = physical_locator
                reason = "primary"
                break 

    print("chosen locator: ", chosen_locator)

    logger.debug(
        f"locate_object: chosen locator with strategy {reason} out of {len(locators.physical_object_locators)}, {request} -> {chosen_locator}"
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
        version_id=chosen_locator.version_id,    # here must use the physical version
        version=chosen_locator.logical_object.id,
    )


@router.post("/start_warmup")
async def start_warmup(
    request: StartWarmupRequest, db: Session = Depends(get_session)
) -> StartWarmupResponse:
    """Given the logical object information and warmup regions, return one or zero physical object locators."""
    
    if request.version_id is not None:
        stmt = (
            select(DBLogicalObject)
            .options(selectinload(DBLogicalObject.physical_object_locators))
            .where(DBLogicalObject.bucket == request.bucket)
            .where(DBLogicalObject.key == request.key)
            .where(DBLogicalObject.status == Status.ready)
            .where(DBLogicalObject.id == request.version_id)    # select the one with specific version
        )
    else:
        stmt = (
            select(DBLogicalObject)
            .options(selectinload(DBLogicalObject.physical_object_locators))
            .where(DBLogicalObject.bucket == request.bucket)
            .where(DBLogicalObject.key == request.key)
            .where(DBLogicalObject.status == Status.ready)
            .order_by(DBLogicalObject.id.desc())    # select the latest version
            #.first()
        )
    locators = (await db.scalars(stmt)).first()
    print("locate_object locators: ", locators)

    if not locators:
        return Response(status_code=404, content="Object Not Found")

    primary_locator = None
    for physical_locator in locators.physical_object_locators:
        if physical_locator.is_primary:
            primary_locator = physical_locator
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
            version=primary_locator.logical_object.id,
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
                version=locator.logical_object.id,  # logical version
            )
            for locator in secondary_locators
        ],
    )


@router.post("/start_upload")
async def start_upload(
    request: StartUploadRequest, db: Session = Depends(get_session)
) -> StartUploadResponse:
    
    version_enabled = (await db.execute(
        select(DBLogicalBucket.version_enabled)
        .where(DBLogicalBucket.bucket == request.bucket)
    )).all()[0]

    if not version_enabled and request.version_id:
        return Response(status_code=400, content="Versioning is not enabled")

    # The case we will contain a version_id are:
    # 1. pull_on_read 
    # 2. copy
    # Under those cases, we will not create new logical objects
    # also do NOT allow repeated upload of the same logical version to the same region
    # When the version is disabled,
    if request.version_id is not None:
        existing_objects_stmt = (
            select(DBLogicalObject)
            .options(selectinload(DBLogicalObject.physical_object_locators))
            .where(DBLogicalObject.bucket == request.bucket)
            .where(DBLogicalObject.key == request.key)
            .where(DBLogicalObject.status == Status.ready)
            .where(DBLogicalObject.id == request.version_id)
        )
    else:
        existing_objects_stmt = (
            select(DBLogicalObject)
            .options(selectinload(DBLogicalObject.physical_object_locators))
            .where(DBLogicalObject.bucket == request.bucket)
            .where(DBLogicalObject.key == request.key)
            .where(DBLogicalObject.status == Status.ready)
            .order_by(DBLogicalObject.id.desc())    # select the latest version
            #.first()
        )

    existing_object = (await db.scalars(existing_objects_stmt)).first()
    print("existing_objects: ", existing_object)
    primary_exists = False
    # Parse results for the object_already_exists check
    if existing_object is not None:
        object_already_exists = any(
            # the exact version has already existed
            locator.location_tag == request.client_from_region and request.version_id is not None
            for locator in existing_object.physical_object_locators
        )

        if object_already_exists:
            logger.error("This exact object already exists")
            return Response(status_code=409, content="Conflict, object already exists")

        # existing_tags = set(locator.location_tag for locator in existing_objects)
        primary_exists = any(locator.is_primary for locator in existing_object.physical_object_locators)

    # version_id is None: should copy the latest version of the object
    # version_id is not None, should copy the corresponding version
    if (request.copy_src_bucket is not None) and (request.copy_src_key is not None):
        if request.version_id is not None:
            copy_src_stmt = (
                select(DBLogicalObject)
                .options(selectinload(DBLogicalObject.physical_object_locators))
                .where(DBLogicalObject.bucket == request.copy_src_bucket)
                .where(DBLogicalObject.key == request.copy_src_key)
                .where(DBLogicalObject.status == Status.ready)
                .where(DBLogicalObject.id == request.version_id)
            )
        else:
            copy_src_stmt = (
                select(DBLogicalObject)
                .options(selectinload(DBLogicalObject.physical_object_locators))
                .where(DBLogicalObject.bucket == request.copy_src_bucket)
                .where(DBLogicalObject.key == request.copy_src_key)
                .where(DBLogicalObject.status == Status.ready)
                .order_by(DBLogicalObject.id.desc())    # select the latest version
                #.first()
            )

        copy_src_locator = (await db.scalars(copy_src_stmt)).first()
        print("copy_src_locator: ", copy_src_locator)
        copy_src_locators_map = {
            locator.location_tag: locator for locator in copy_src_locator.physical_object_locators
        }
        copy_src_locations = set(locator.location_tag for locator in copy_src_locator.physical_object_locators)
    else:
        copy_src_locations = None

    if existing_object is None:
        print("existing_objects is None")
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
        logical_object = existing_object

        # NOT copy-on-read, NOT copy,
        # And the version must be enabled
        if request.policy != "copy_on_read" and ((request.copy_src_bucket is None) or (request.copy_src_key is None)) and version_enabled:
            # version support: create a new logical object based on the previous logical object fields (the previous newest version)
            logical_object = DBLogicalObject(
                bucket=logical_object.bucket,
                key=logical_object.key,
                size=logical_object.size,
                last_modified=logical_object.last_modified,  # this field should be the current timestamp, here we can just ignore since this will be updated in the complete upload step
                etag=logical_object.etag,
                status=Status.pending,
                multipart_upload_id=logical_object.multipart_upload_id,
                # version=logical_object.version
                # + 1,  # increment version_id for each logical object
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

    # when enabling versioning, primary exist does not equal to the pull-on-read case
    # make sure we use copy-on-read policy
    if primary_exists and request.policy == "copy_on_read":
        print("primary_exists")
        # Assume that physical bucket locators for this region already exists and we don't need to create them
        # For pull-on-read
        upload_to_region_tags = [request.client_from_region]
        primary_write_region = [
            locator.location_tag for locator in existing_object.physical_object_locators if locator.is_primary
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
            print("upload_to_region_tags: ", upload_to_region_tags)
            primary_write_region = [
                locator.location_tag
                for locator in physical_bucket_locators
                if locator.is_primary
            ]
            print("primary_write_region: ", primary_write_region)
            assert (
                len(primary_write_region) == 1
            ), "should only have one primary write region"
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
        # if region_tag in existing_tags:
        #     continue

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
                logical_object=logical_object,  # link the physical object with the logical object
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
        print("locators: ", locators)

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
                version_id=locator.version_id,
                verison=locator.logical_object.id,
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

    print("physical_locator-logical object: ", physical_locator.logical_object)

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
        print("logical_object: ", logical_object)
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
    # NOTE: currently, always choose the newest version
    stmt = (
        select(DBLogicalObject)
        .options(selectinload(DBLogicalObject.physical_object_locators))
        .where(DBLogicalObject.bucket == request.bucket)
        .where(DBLogicalObject.key == request.key)
        # .where(DBLogicalObject.status == Status.pending)
        .where(DBLogicalObject.multipart_upload_id == request.multipart_upload_id)
        .order_by(DBLogicalObject.id.desc())    # select the latest version
        #.first()
    )
    locators = (await db.scalars(stmt)).first().physical_object_locators
    if len(locators) == 0:
        return Response(status_code=404, content="Not Found")
    copy_src_buckets, copy_src_keys = [], []
    
    if request.copy_src_bucket is not None and request.copy_src_key is not None:
        physical_src_locators = (
            await db.scalars(
                select(DBLogicalObject)
                .options(selectinload(DBLogicalObject.physical_object_locators))
                .where(DBLogicalObject.bucket == request.copy_src_bucket)
                .where(DBLogicalObject.key == request.copy_src_key)
                .where(DBLogicalObject.status == Status.ready)
                .order_by(DBLogicalObject.id.desc())    # select the latest version
                #.first()
            )
        ).first().physical_object_locators

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
            func.max(DBLogicalObject.id),   # id is the version
            DBLogicalObject.bucket,
            DBLogicalObject.key,
            DBLogicalObject.size,
            DBLogicalObject.etag,
            DBLogicalObject.last_modified,
            DBLogicalObject.status,
            DBLogicalObject.multipart_upload_id,
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
    objects_all = objects.all() # NOTE: DO NOT use `scalars` here

    for i, obj in enumerate(objects_all):
        print(f"list obj {i}: ", obj)

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

# NOTE: This function is only for testing currently.
# We can consdier add a `list-object-versions` function in the proxy side 
# and call this function.
@router.post("/list_objects_versioning")
async def list_objects_versioning(
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

    objects = await db.scalars(stmt)
    objects_all = objects.all() 

    for i, obj in enumerate(objects_all):
        print(f"list obj {i}: ", obj)

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
            version_id=obj.id,
        )
        for obj in objects_all
    ]

@router.post("/head_object")
async def head_object(
    request: HeadObjectRequest, db: Session = Depends(get_session)
) -> HeadObjectResponse:
    if request.version_id is not None:
        stmt = (
            select(DBLogicalObject)
            .where(DBLogicalObject.bucket == request.bucket)
            .where(DBLogicalObject.key == request.key)
            .where(DBLogicalObject.status == Status.ready)
            .where(DBLogicalObject.id == request.version_id)
        )
    else:
        stmt = (
            select(DBLogicalObject)
            .where(DBLogicalObject.bucket == request.bucket)
            .where(DBLogicalObject.key == request.key)
            .where(DBLogicalObject.status == Status.ready)
            .order_by(DBLogicalObject.id.desc())    # select the latest version
            #.first()
        )

    logical_object = (await db.scalars(stmt)).first()

    if logical_object is None:
        return Response(status_code=404, content="Not Found")

    logger.debug(f"head_object: {request} -> {logical_object}")

    return HeadObjectResponse(
        bucket=logical_object.bucket,
        key=logical_object.key,
        size=logical_object.size,
        etag=logical_object.etag,
        last_modified=logical_object.last_modified,
        version_id=logical_object.id,
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
    # objects_dict = {}
    # for obj in objects:
    #     if obj.key not in objects_dict or (
    #         obj.key in objects_dict and objects_dict[obj.key].version < obj.version
    #     ):
    #         objects_dict[obj.key] = obj
    # objects = list(objects_dict.values())

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

    # for object in objects:
    #     await db.refresh(object, ["multipart_upload_parts"])

    # # only keep the latest version of the object
    # # TODO: change to sort method of list
    # objects_dict = {}
    # for obj in objects:
    #     if obj.key not in objects_dict or (
    #         obj.key in objects_dict and objects_dict[obj.key].version < obj.version
    #     ):
    #         objects_dict[obj.key] = obj
    # objects = list(objects_dict.values())

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
) -> List[ObjectStatus]:
    """Given the logical object information, return the status of the object.
    Currently only used for testing metadata cleanup."""
    object_status_lst = []
    if request.version_id is not None:
        stmt= (
            select(DBLogicalObject)
            .options(selectinload(DBLogicalObject.physical_object_locators))
            .where(DBLogicalObject.bucket == request.bucket)
            .where(DBLogicalObject.key == request.key)   
            .where(DBLogicalObject.id == request.version_id)    # select the one with specific version
        )
    else:
        stmt = (
            select(DBLogicalObject)
            .options(selectinload(DBLogicalObject.physical_object_locators))
            .where(DBLogicalObject.bucket == request.bucket)
            .where(DBLogicalObject.key == request.key)
        )
    # get physical locators
    locators = (await db.scalars(stmt)).all()
    if len(locators) == 0:
        return Response(status_code=404, content="Object Not Found")

    chosen_locator = []
    reason = ""

    for logical_object in locators:
        await db.refresh(logical_object, ["physical_object_locators"])
        for physical_locator in logical_object.physical_object_locators:
            if physical_locator.location_tag == request.client_from_region:
                chosen_locator.append(physical_locator)
                reason = "exact match"
                break

        if len(chosen_locator) == 0:
            # find the primary locator
            for physical_locator in logical_object.physical_object_locators:
                if physical_locator.is_primary:
                    chosen_locator.append(physical_locator)
                    reason = "fallback to primary"
                    break

    logger.debug(
        f"locate_object: chosen locator with strategy {reason} out of {len(locators)}, {request} -> {chosen_locator}"
    )

    for locator in chosen_locator:
        object_status_lst.append(ObjectStatus(status=locator.status))
    # return object status
    return object_status_lst
