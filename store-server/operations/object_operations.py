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
    DeleteMarker,
    SetPolicyRequest,
)
from operations.schemas.bucket_schemas import DBLogicalBucket
from sqlalchemy.orm import selectinload, Session, joinedload, lazyload
from itertools import zip_longest
from sqlalchemy.sql import select
from sqlalchemy import or_, text, and_
from sqlalchemy import func
from operations.utils.conf import Status
from fastapi import APIRouter, Response, Depends, status
from operations.utils.db import get_session, logger
from typing import List
from datetime import datetime
from itertools import chain
from operations.policy.placement_policy import (
    put_policy,
    SingleRegionWrite,
    ReplicateAll,
    PushonWrite,
    PullOnRead,
    LocalWrite,
)
from .policy.transfer_policy import (
    get_policy,
    DirectTransfer,
    CheapestTransfer,
    ClosestTransfer,
)


router = APIRouter()


@router.post("/update_policy")
async def update_policy(
    request: SetPolicyRequest, db: Session = Depends(get_session)
) -> None:
    global get_policy, put_policy
    put_policy_type = request.put_policy
    get_policy_type = request.get_policy

    old_put_policy_type = put_policy.name()
    old_get_policy_type = get_policy.name()

    if put_policy_type is None and get_policy_type is None:
        raise ValueError("Invalid policy type")

    if put_policy_type is not None and put_policy_type != old_put_policy_type:
        if put_policy_type == "write_local":
            put_policy = LocalWrite()
        elif put_policy_type == "single_region":
            put_policy = SingleRegionWrite()
        elif put_policy_type == "copy_on_read":
            put_policy = PullOnRead()
        elif put_policy_type == "replicate_all":
            put_policy = ReplicateAll()
        elif put_policy_type == "push":
            put_policy = PushonWrite()
        else:
            raise ValueError("Invalid placement policy type")
    if get_policy_type is not None and get_policy_type != old_get_policy_type:
        if get_policy_type == "direct":
            get_policy = DirectTransfer()
        elif get_policy_type == "cheapest":
            get_policy = CheapestTransfer()
        elif get_policy_type == "closest":
            get_policy = ClosestTransfer()
        else:
            raise ValueError("Invalid transfer policy type")


# TODO: when creating new logical object, we need to consider different put policy
@router.post("/start_delete_objects")
async def start_delete_objects(
    request: DeleteObjectsRequest, db: Session = Depends(get_session)
) -> DeleteObjectsResponse:
    # measure the time
    start = datetime.now()

    await db.execute(text("BEGIN IMMEDIATE;"))

    # await db.execute(text("LOCK TABLE logical_objects IN ACCESS EXCLUSIVE MODE;"))

    version_enabled = (
        await db.execute(
            select(DBLogicalBucket.version_enabled).where(
                DBLogicalBucket.bucket == request.bucket
            )
        )
    ).all()[0][0]

    specific_version = any(
        len(request.object_identifiers[key]) > 0 for key in request.object_identifiers
    )

    end = datetime.now()

    print("start delete objects: version setting detection: ", end - start)

    if version_enabled is None and specific_version:
        return Response(status_code=400, content="Versioning is not enabled")

    if request.multipart_upload_ids and len(request.object_identifiers) != len(
        request.multipart_upload_ids
    ):
        return Response(
            status_code=400,
            content="Mismatched lengths for ids and multipart_upload_ids",
        )

    locator_dict = {}
    delete_marker_dict = {}
    op_type = {}
    for key, multipart_upload_id in zip_longest(
        request.object_identifiers, request.multipart_upload_ids or []
    ):
        start = datetime.now()

        if multipart_upload_id:
            stmt = (
                select(DBLogicalObject)
                .options(selectinload(DBLogicalObject.physical_object_locators))
                .where(DBLogicalObject.bucket == request.bucket)
                .where(DBLogicalObject.key == key)
                .where(
                    or_(
                        DBLogicalObject.status == Status.ready,
                        DBLogicalObject.status == Status.pending,
                    )
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
                .order_by(DBLogicalObject.id.desc())
            )
        # multiple versioning support
        logical_objs = (await db.scalars(stmt)).all()

        end = datetime.now()

        print(
            "start delete objects: query logical objects with key {} in loop: ".format(
                key
            ),
            end - start,
        )

        if len(logical_objs) == 0:
            return Response(status_code=404, content="Objects not found")

        version_suspended = logical_objs[0].version_suspended

        locators = []
        replaced = False
        add_obj = False
        pre_logical_obj = None
        # since we have ordered the logical objects before traversing them
        # the first one will be the most recent one
        # and if there is a version-suspended marker, it will be the first one
        # if it's a multipart, the upload_id is unique, so we can just delete the first one

        start = datetime.now()

        for idx, logical_obj in enumerate(logical_objs):
            # Follow the semantics of S3:
            # Check it here:
            # https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeletingObjectVersions.html
            # https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeletingObjectsfromVersioningSuspendedBuckets.html
            # https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeleteMarker.html
            # https://docs.aws.amazon.com/AmazonS3/latest/userguide/ManagingDelMarkers.html

            # simple delete
            if len(request.object_identifiers[key]) == 0 and idx == 0:
                if version_enabled is True or (
                    version_enabled is False and version_suspended is False
                ):
                    # under both these cases, in S3 semantics, we will add things to the DB and doesn't touch the older ones
                    # so here, we need to add new logical object and physical object locators
                    # This also indicates, when we returning back to the complete_delete_objects function,
                    # The status of objects that need to be updated will be these newly created objects
                    # rather than the older ones. So what will be included in the returned locators list
                    # will be the newly created ones.
                    pre_logical_obj = logical_obj
                    # insert a delete marker
                    logical_obj = create_logical_object(
                        logical_obj,
                        request,
                        version_suspended=(version_enabled is not True),
                        delete_marker=True,
                    )
                    db.add(logical_obj)
                    new_physical_locators = []
                    # need to also add new physical object locators
                    for physical_locator in pre_logical_obj.physical_object_locators:
                        new_physical_locators.append(
                            DBPhysicalObjectLocator(
                                logical_object=logical_obj,
                                location_tag=physical_locator.location_tag,
                                cloud=physical_locator.cloud,
                                region=physical_locator.region,
                                bucket=physical_locator.bucket,
                                key=physical_locator.key,
                                status=Status.pending,
                                is_primary=physical_locator.is_primary,
                                # version_id=physical_locators.version_id,
                            )
                        )
                    db.add_all(new_physical_locators)
                    add_obj = True
                elif version_enabled is False and version_suspended is True:
                    # remove the null version and replace the one with a delete marker
                    # NOTE: The obj being removed can also be a delete marker with null version
                    # We just need to update the metadata of the existing objects, also don't remove anything in the DB
                    logical_obj.delete_marker = True
                    replaced = True
            # For the case adding new objs to the DB, we need to commit first
            # in order to use them in the following traversal
            if add_obj:
                await db.commit()

            if len(request.object_identifiers[key]) > 0 and (
                logical_obj.id not in request.object_identifiers[key]
            ):
                continue  # skip if the version_id is not in the request

            for physical_locator, pre_physical_locator in zip_longest(
                logical_obj.physical_object_locators,
                pre_logical_obj.physical_object_locators if pre_logical_obj else [],
            ):
                await db.refresh(physical_locator, ["logical_object"])

                if (
                    not add_obj
                    and physical_locator.status not in Status.ready
                    and not multipart_upload_id
                ):
                    logger.error(
                        f"Cannot delete physical object. Current status is {physical_locator.status}"
                    )
                    return Response(
                        status_code=409,
                        content="Cannot delete physical object in current state",
                    )

                if not add_obj and not replaced:
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
                        # the version_id should be the one that we want the client to operate on
                        # so when we add objects into the DB, we still want the client to cope with the previous version
                        version_id=physical_locator.version_id
                        if pre_physical_locator is None
                        else pre_physical_locator.version_id,
                        version=physical_locator.logical_object.id
                        if version_enabled is not None
                        else None,
                    )
                )

            if not add_obj and not replaced:
                logical_obj.status = Status.pending_deletion

            try:
                await db.commit()
            except Exception as e:
                logger.error(f"Error occurred while committing changes: {e}")
                return Response(status_code=500, content="Error committing changes")

            logger.debug(f"start_delete_object: {request} -> {logical_obj}")

            # for these cases, we only need to deal with the first logical object
            if replaced or add_obj:
                break

        end = datetime.now()

        print(
            "start delete objects: delete corresponding physical objects with key {} in loop: ".format(
                key
            ),
            end - start,
        )

        print("version setting: ", version_enabled)
        print("logical_obj.id: ", logical_obj.id)

        locator_dict[key] = locators
        delete_marker_dict[key] = DeleteMarker(
            delete_marker=logical_obj.delete_marker,
            version_id=None
            if logical_obj.version_suspended or version_enabled is None
            else str(logical_obj.id),
        )
        if add_obj:
            op_type[key] = "add"
        elif replaced:
            op_type[key] = "replace"
        else:
            op_type[key] = "delete"

    return DeleteObjectsResponse(
        locators=locator_dict,
        delete_markers=delete_marker_dict,
        op_type=op_type,
    )


@router.patch("/complete_delete_objects")
async def complete_delete_objects(
    request: DeleteObjectsIsCompleted, db: Session = Depends(get_session)
):
    # For the version support, we need to perform different operations
    # based on the initial delete op type
    # If the op type is delete, we need to delete the physical object locators and logical objects possibly
    # If the op type is replace, we don't need to do anything
    # If the op type is add, we need to update the metadata of the existing objects from pending to ready

    # TODO: need to deal with partial failures
    if request.multipart_upload_ids and len(request.ids) != len(
        request.multipart_upload_ids
    ):
        return Response(
            status_code=400,
            content="Mismatched lengths for ids and multipart_upload_ids",
        )

    for idx, (id, multipart_upload_id, op_type) in enumerate(
        zip_longest(
            request.ids,
            request.multipart_upload_ids or [],
            request.op_type,
        )
    ):
        if op_type == "delete":
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
                    status_code=409,
                    content="Physical object is not marked for deletion",
                )

            await db.delete(physical_locator)

            # only delete the logical object with same version if there is no other physical object locator if possible
            remaining_physical_locators_stmt = select(DBPhysicalObjectLocator).where(
                DBPhysicalObjectLocator.logical_object_id
                == physical_locator.logical_object.id
            )
            remaining_physical_locators = await db.execute(
                remaining_physical_locators_stmt
            )
            if not remaining_physical_locators.all():
                await db.delete(physical_locator.logical_object)

        elif op_type == "replace":
            continue
        elif op_type == "add":
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

            if physical_locator.status != Status.pending:
                return Response(
                    status_code=409, content="Physical object is not marked for pending"
                )

            physical_locator.status = Status.ready
            physical_locator.lock_acquired_ts = None

            # if this is the first physical locators we cope with linked with some
            # logical objects whose status is pending, we change the status of the logical object
            # to ready

            if idx == 0:
                stmt = select(DBLogicalObject).where(
                    DBLogicalObject.id == physical_locator.logical_object_id
                )

                logical_obj = await db.scalar(stmt)

                if logical_obj is None:
                    logger.error(f"logical object not found: {request}")
                    return Response(status_code=404, content="Logical Object Not Found")

                logical_obj.status = Status.ready

        else:
            logger.error(f"Invalid op_type: {op_type}")
            return Response(status_code=400, content="Invalid op_type")

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

    version_enabled = (
        await db.execute(
            select(DBLogicalBucket.version_enabled).where(
                DBLogicalBucket.bucket == request.bucket
            )
        )
    ).all()[0][0]

    if version_enabled is None and request.version_id:
        return Response(status_code=400, content="Versioning is not enabled")

    stmt = (
        select(DBLogicalObject)
        .join(DBPhysicalObjectLocator)
        .where(
            and_(
                DBLogicalObject.bucket == request.bucket,
                DBLogicalObject.key == request.key,
                DBLogicalObject.status == Status.ready,
                DBPhysicalObjectLocator.status == Status.ready,
                DBLogicalObject.id == request.version_id
                if request.version_id is not None
                else True,
            )
        )
        .order_by(None if request.version_id is not None else DBLogicalObject.id.desc())
    )

    locators = (await db.scalars(stmt)).first()

    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeletingObjectVersions.html
    if locators is None or (locators.delete_marker and not request.version_id):
        return Response(status_code=404, content="Object Not Found")

    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeleteMarker.html
    if locators and locators.delete_marker and request.version_id:
        return Response(status_code=405, content="Not allowed to get a delete marker")

    # if request.get_primary:
    #     chosen_locator = next(locator for locator in locators if locator.is_primary)
    #     reason = "exact match (primary)"
    # else:
    await db.refresh(locators, ["physical_object_locators"])

    chosen_locator = get_policy.get(request, locators.physical_object_locators)

    logger.debug(
        f"locate_object: chosen locator out of {len(locators.physical_object_locators)}, {request} -> {chosen_locator}"
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
        version_id=chosen_locator.version_id,  # here must use the physical version
        version=chosen_locator.logical_object.id
        if version_enabled is not None
        else None,
    )


@router.post("/start_warmup")
async def start_warmup(
    request: StartWarmupRequest, db: Session = Depends(get_session)
) -> StartWarmupResponse:
    """Given the logical object information and warmup regions, return one or zero physical object locators."""

    version_enabled = (
        await db.execute(
            select(DBLogicalBucket.version_enabled).where(
                DBLogicalBucket.bucket == request.bucket
            )
        )
    ).all()[0][0]

    if version_enabled is None and request.version_id:
        return Response(status_code=400, content="Versioning is not enabled")

    if request.version_id is not None:
        stmt = (
            select(DBLogicalObject)
            .options(selectinload(DBLogicalObject.physical_object_locators))
            .where(DBLogicalObject.bucket == request.bucket)
            .where(DBLogicalObject.key == request.key)
            .where(DBLogicalObject.status == Status.ready)
            .where(
                DBLogicalObject.id == request.version_id
            )  # select the one with specific version
        )
    else:
        stmt = (
            select(DBLogicalObject)
            .options(selectinload(DBLogicalObject.physical_object_locators))
            .where(DBLogicalObject.bucket == request.bucket)
            .where(DBLogicalObject.key == request.key)
            .where(DBLogicalObject.status == Status.ready)
            .order_by(DBLogicalObject.id.desc())  # select the latest version
            # .first()
        )
    locators = (await db.scalars(stmt)).first()

    if locators is None:
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
            version=primary_locator.logical_object.id
            if version_enabled is not None
            else None,
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
                version=locator.logical_object.id
                if version_enabled is not None
                else None,  # logical version
            )
            for locator in secondary_locators
        ],
    )


@router.post("/start_upload")
async def start_upload(
    request: StartUploadRequest, db: Session = Depends(get_session)
) -> StartUploadResponse:
    await db.execute(text("PRAGMA journal_mode=WAL;"))
    await db.execute(text("PRAGMA synchronous=OFF;"))
    await db.execute(text("BEGIN IMMEDIATE;"))

    version_enabled, logical_bucket = (
        await db.execute(
            select(DBLogicalBucket.version_enabled, DBLogicalBucket)
            .where(DBLogicalBucket.bucket == request.bucket)
            .options(joinedload(DBLogicalBucket.physical_bucket_locators))
        )
    ).unique().one_or_none()

    # we can still provide version_id when version_enalbed is False (corresponding to the `Suspended`)
    # status in S3
    if version_enabled is None and request.version_id:
        return Response(
            status_code=400,
            content="Versioning is NULL, make sure you enable versioning first.",
        )

    # The case we will contain a version_id are:
    # 1. pull_on_read: don't create new logical objects
    # 2. copy: create new logical objects in dst locations
    existing_objects_stmt = (
        select(DBLogicalObject)
        .where(
            and_(
                DBLogicalObject.bucket == request.bucket,
                DBLogicalObject.key == request.key,
                or_(
                    DBLogicalObject.status == Status.ready,
                    DBLogicalObject.status == Status.pending,
                ),
                DBLogicalObject.id == request.version_id
                if request.version_id is not None
                else True,
            )
        )
        .order_by(DBLogicalObject.id.desc() if request.version_id is None else None)
        .options(joinedload(DBLogicalObject.physical_object_locators))
    )

    existing_object = (await db.scalars(existing_objects_stmt)).unique().first()

    # if we want to perform copy or pull-on-read and the source object does not exist, we should return 404
    if (
        request.version_id
        and not existing_object
        and (request.copy_src_bucket is None or put_policy.name() == "copy_on_read")
    ):
        return Response(
            status_code=404,
            content="Object of version {} Not Found".format(request.version_id),
        )

    # Parse results for the object_already_exists check
    primary_exists = False
    existing_tags = ()
    if existing_object is not None:
        object_already_exists = any(
            locator.location_tag == request.client_from_region
            for locator in existing_object.physical_object_locators
        )

        # allow create new logical objects in the same region or overwrite current version when versioning is enabled/suspended
        if object_already_exists and version_enabled is None:
            logger.error("This exact object already exists")
            return Response(status_code=409, content="Conflict, object already exists")

        existing_tags = {
            locator.location_tag: locator.id
            for locator in existing_object.physical_object_locators
        }
        primary_exists = any(
            locator.is_primary for locator in existing_object.physical_object_locators
        )

    # version_id is None: should copy the latest version of the object
    # version_id is not None, should copy the corresponding version
    if (request.copy_src_bucket is not None) and (request.copy_src_key is not None):
        copy_src_stmt = (
            select(DBLogicalObject)
            .where(
                and_(
                    DBLogicalObject.bucket == request.copy_src_bucket,
                    DBLogicalObject.key == request.copy_src_key,
                    DBLogicalObject.status == Status.ready,
                    DBLogicalObject.id == request.version_id
                    if request.version_id is not None
                    else True,
                )
            )
            .order_by(DBLogicalObject.id.desc() if request.version_id is None else None)
            .options(joinedload(DBLogicalObject.physical_object_locators))
        )

        copy_src_locator = (await db.scalars(copy_src_stmt)).unique().first()

        # https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeletingObjectVersions.html
        if copy_src_locator is None or (
            copy_src_locator.delete_marker and not request.version_id
        ):
            return Response(status_code=404, content="Object Not Found")

        # https://docs.aws.amazon.com/cli/latest/reference/s3api/upload-part-copy.html
        if copy_src_locator and copy_src_locator.delete_marker and request.version_id:
            return Response(
                status_code=400, content="Not allowed to copy from a delete marker"
            )

        copy_src_locators_map = {
            locator.location_tag: locator
            for locator in copy_src_locator.physical_object_locators
        }
        copy_src_locations = set(
            locator.location_tag
            for locator in copy_src_locator.physical_object_locators
        )
    else:
        copy_src_locations = None

    # The following logic includes creating logical objects of new version mimicing the S3 semantics.
    # Check here:
    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/AddingObjectstoVersionSuspendedBuckets.html
    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/AddingObjectstoVersioningEnabledBuckets.html
    if existing_object is None:
        logical_object = create_logical_object(
            existing_object, request, version_suspended=(not version_enabled)
        )
        db.add(logical_object)

    # If the object already exists, we need to check the version_enabled field:
    # version_enabled is NULL, use existing object
    # version_enabled is enabled,
    #   - if performing copy-on-read, use existing object
    #   - Otherwise, create new logical object
    # version_enabled is suspended,
    #   - if performing copy-on-read, use existing object
    #   - Otherwise, check version_suspended field, - if False, create new logical object and set the field to be False
    #   - if True, use existing object (overwrite the existing object)

    elif put_policy.name() == "copy_on_read" or (
        version_enabled is None or existing_object.version_suspended
    ):
        logical_object = existing_object
        logical_object.delete_marker = False
    else:
        logical_object = create_logical_object(
            existing_object, request, version_suspended=not version_enabled
        )
        db.add(logical_object)

    physical_bucket_locators = logical_bucket.physical_bucket_locators

    primary_write_region = None

    # when enabling versioning, primary exist does not equal to the pull-on-read case
    # make sure we use copy-on-read policy
    if primary_exists and put_policy.name() == "copy_on_read":
        # Assume that physical bucket locators for this region already exists and we don't need to create them
        # For pull-on-read
        upload_to_region_tags = put_policy.place(request)
        primary_write_region = [
            locator.location_tag
            for locator in existing_object.physical_object_locators
            if locator.is_primary
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
        if put_policy.name() == "push":
            # Except this case, always set the first-write region of the OBJECT to be primary
            upload_to_region_tags = put_policy.place(request, physical_bucket_locators)
            primary_write_region = [
                locator.location_tag
                for locator in physical_bucket_locators
                if locator.is_primary
            ]
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
            copy_src_locators_map[tag].bucket for tag in copy_src_locations
        ]
        copy_src_keys = [copy_src_locators_map[tag].key for tag in copy_src_locations]

        logger.debug(
            f"start_upload: copy_src_locations={copy_src_locations}, "
            f"upload_to_region_tags={upload_to_region_tags}, "
            f"copy_src_buckets={copy_src_buckets}, "
            f"copy_src_keys={copy_src_keys}"
        )
    locators = []
    existing_locators = []
    
    for region_tag in upload_to_region_tags:
        # If the version_enabled is NULL, we should skip the existing tags, only create new physical object locators for the newly upload regions
        # If the version_enabled is True, we should create new physical object locators for the newly created logical object no matter what regions we are trying to upload
        # If the version_enabled is False, we should either create new physical object locators for the newly created logical object
        # or overwrite the existing physical object locators for the existing logical object depending on the version_suspended field
        if region_tag in existing_tags and version_enabled is None:
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

        # For the region not in the existing tags, we need to create new physical object locators and add them to the DB
        # For the region in the existing tags, we need to decide whether creating new physical object locators or not based on:
        # - version_enabled is NULL, we already skipped them in the above if condition
        # - version_enabled is True, we should create new physical object locators for them
        # - Otherwise, check version_suspended field,
        # - if False, create new physical object locators
        # - if True, update existing physical object (but DO NOT add them to the DB)

        if (
            region_tag not in existing_tags
            or version_enabled
            or (existing_object and existing_object.version_suspended is False)
        ):
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
        else:
            existing_locators.append(
                DBPhysicalObjectLocator(
                    id=existing_tags[region_tag],
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
                version=locator.logical_object.id
                if version_enabled is not None
                else None,
            )
            for locator in chain(locators, existing_locators)
        ],
        copy_src_buckets=copy_src_buckets,
        copy_src_keys=copy_src_keys,
    )


@router.patch("/complete_upload")
async def complete_upload(
    request: PatchUploadIsCompleted, db: Session = Depends(get_session)
):
    await db.execute(text("PRAGMA journal_mode=WAL;"))
    await db.execute(text("PRAGMA synchronous=OFF;"))
    stmt = (
        select(DBPhysicalObjectLocator)
        .where(DBPhysicalObjectLocator.id == request.id)
        .options(joinedload(DBPhysicalObjectLocator.logical_object))
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
    policy_name = put_policy.name()
    if (
        (policy_name == "push" and physical_locator.is_primary)
        or policy_name == "write_local"
        or policy_name == "copy_on_read"
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
    print("start time: ", datetime.now())

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

    print("end time: ", datetime.now())


@router.post("/continue_upload")
async def continue_upload(
    request: ContinueUploadRequest, db: Session = Depends(get_session)
) -> List[ContinueUploadResponse]:
    print("start time: ", datetime.now())
    version_enabled = (
        await db.execute(
            select(DBLogicalBucket.version_enabled).where(
                DBLogicalBucket.bucket == request.bucket
            )
        )
    ).all()[0][0]

    if version_enabled is None and request.version_id:
        return Response(status_code=400, content="Versioning is not enabled")

    # the upload_id is a unique identifer
    stmt = (
        select(DBLogicalObject)
        .options(selectinload(DBLogicalObject.physical_object_locators))
        .where(DBLogicalObject.bucket == request.bucket)
        .where(DBLogicalObject.key == request.key)
        .where(DBLogicalObject.status == Status.pending)
        .where(DBLogicalObject.multipart_upload_id == request.multipart_upload_id)
        .order_by(DBLogicalObject.id.desc())
        # .first()
    )
    locators = (await db.scalars(stmt)).first()
    if locators is None:
        return Response(status_code=404, content="Not Found")

    locators = locators.physical_object_locators

    copy_src_buckets, copy_src_keys = [], []

    # cope with upload_part_copy
    if request.copy_src_bucket is not None and request.copy_src_key is not None:
        physical_src_locators = (
            (
                await db.scalars(
                    select(DBLogicalObject)
                    .options(selectinload(DBLogicalObject.physical_object_locators))
                    .where(
                        and_(
                            DBLogicalObject.bucket == request.copy_src_bucket,
                            DBLogicalObject.key == request.copy_src_key,
                            DBLogicalObject.status == Status.ready,
                            DBLogicalObject.id == request.version_id
                            if request.version_id is not None
                            else True,
                        )
                    )
                    .order_by(
                        DBLogicalObject.id.desc()
                        if request.version_id is None
                        else None
                    )
                )
            )
            .first()
            .physical_object_locators
        )

        if len(physical_src_locators) == 0:
            return Response(status_code=404, content="Source object Not Found")

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

    res = [
        ContinueUploadResponse(
            id=locator.id,
            tag=locator.location_tag,
            cloud=locator.cloud,
            bucket=locator.bucket,
            region=locator.region,
            key=locator.key,
            multipart_upload_id=locator.multipart_upload_id,
            # version=locator.logical_object.id if version_enabled is not None else None,
            version_id=locator.version_id,
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

    print("end time: ", datetime.now())
    return res


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

    conditions = [
        DBLogicalObject.bucket == logical_bucket.bucket,
        DBLogicalObject.status == Status.ready,
        DBLogicalObject.delete_marker is not True,  # Exclude delete markers
    ]

    if request.prefix is not None:
        conditions.append(DBLogicalObject.key.startswith(request.prefix))
    if request.start_after is not None:
        conditions.append(DBLogicalObject.key > request.start_after)

    stmt = (
        select(
            func.max(DBLogicalObject.id),
            DBLogicalObject.bucket,
            DBLogicalObject.key,
            DBLogicalObject.size,
            DBLogicalObject.etag,
            DBLogicalObject.last_modified,
            DBLogicalObject.status,
            DBLogicalObject.multipart_upload_id,
        )
        .where(*conditions)
        .group_by(DBLogicalObject.key)
        .order_by(DBLogicalObject.key)
    )

    if request.max_keys is not None:
        stmt = stmt.limit(request.max_keys)

    objects = await db.execute(stmt)
    objects_all = objects.all()  # NOTE: DO NOT use `scalars` here

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
    print("head_object: ", request)

    version_enabled = (
        await db.execute(
            select(DBLogicalBucket.version_enabled).where(
                DBLogicalBucket.bucket == request.bucket
            )
        )
    ).all()[0][0]

    if version_enabled is None and request.version_id:
        return Response(status_code=400, content="Versioning is not enabled")

    stmt = (
        select(DBLogicalObject)
        .where(
            and_(
                DBLogicalObject.bucket == request.bucket,
                DBLogicalObject.key == request.key,
                DBLogicalObject.status == Status.ready,
                DBLogicalObject.id == request.version_id
                if request.version_id is not None
                else True,
            )
        )
        .order_by(DBLogicalObject.id.desc() if request.version_id is None else None)
    )

    logical_object = (await db.scalars(stmt)).first()

    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeletingObjectVersions.html
    if logical_object is None or (
        logical_object.delete_marker and not request.version_id
    ):
        return Response(status_code=404, content="Object Not Found")

    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeleteMarker.html
    if logical_object and logical_object.delete_marker and request.version_id:
        return Response(status_code=405, content="Not allowed to get a delete marker")

    logger.debug(f"head_object: {request} -> {logical_object}")

    return HeadObjectResponse(
        bucket=logical_object.bucket,
        key=logical_object.key,
        size=logical_object.size,
        etag=logical_object.etag,
        last_modified=logical_object.last_modified,
        version_id=logical_object.id if version_enabled is not None else None,
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

    version_enabled = (
        await db.execute(
            select(DBLogicalBucket.version_enabled).where(
                DBLogicalBucket.bucket == request.bucket
            )
        )
    ).all()[0][0]

    if version_enabled is None and request.version_id:
        return Response(status_code=400, content="Versioning is not enabled")

    object_status_lst = []
    if request.version_id is not None:
        stmt = (
            select(DBLogicalObject)
            .options(selectinload(DBLogicalObject.physical_object_locators))
            .where(DBLogicalObject.bucket == request.bucket)
            .where(DBLogicalObject.key == request.key)
            .where(
                DBLogicalObject.id == request.version_id
            )  # select the one with specific version
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


def create_logical_object(
    existing_object, request, version_suspended=False, delete_marker=False
):
    return DBLogicalObject(
        bucket=existing_object.bucket if existing_object else request.bucket,
        key=existing_object.key if existing_object else request.key,
        size=existing_object.size if existing_object else None,
        last_modified=existing_object.last_modified
        if existing_object
        else None,  # to be updated later
        etag=existing_object.etag if existing_object else None,
        status=Status.pending,
        multipart_upload_id=(
            existing_object.multipart_upload_id
            if existing_object
            else (uuid.uuid4().hex if request.is_multipart else None)
        ),
        version_suspended=version_suspended,
        delete_marker=delete_marker,
    )

