import asyncio
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import select, update, and_

from fastapi import FastAPI
from fastapi.routing import APIRoute
from operations.utils.conf import Base
from operations.schemas.object_schemas import (
    DBLogicalObject,
    DBPhysicalObjectLocator,
    Status,
    HealthcheckResponse,
)
from operations.schemas.bucket_schemas import DBLogicalBucket, DBPhysicalBucketLocator
from operations.bucket_operations import router as bucket_operations_router
from operations.object_operations import router as object_operations_router
from operations.utils.db import engine


app = FastAPI()

load_dotenv()
app.include_router(bucket_operations_router)
app.include_router(object_operations_router)

stop_task_flag = asyncio.Event()
background_tasks = set()


async def rm_lock_on_timeout(minutes: int = 10, test: bool = False):
    # initial wait to prevent first check which should never run
    if not test:
        await asyncio.sleep(minutes)
    while not stop_task_flag.is_set() or test:
        async with engine.begin() as db:
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
                        .join(
                            DBLogicalObject,
                            and_(
                                DBLogicalObject.id == DBPhysicalObjectLocator.logical_object_id,
                                DBLogicalObject.version
                                == DBPhysicalObjectLocator.logical_object_version,
                            ),
                        )
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

        if test:
            break

        await asyncio.sleep(minutes * 60)


@app.on_event("shutdown")
async def shutdown_event():
    # Set the flag to signal the background task to stop
    stop_task_flag.set()
    background_tasks.discard


@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # await conn.exec_driver_sql("pragma journal_mode=memory")
        # await conn.exec_driver_sql("pragma synchronous=OFF")

    task = asyncio.create_task(rm_lock_on_timeout())
    background_tasks.add(task)


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
