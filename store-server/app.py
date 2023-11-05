from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.routing import APIRoute
from operations.utils.conf import Base
from operations.schemas.object_schemas import HealthcheckResponse
from operations.bucket_operations import router as bucket_operations_router
from operations.object_operations import router as object_operations_router
from operations.utils.db import engine  # Import these from the db module now

app = FastAPI()

load_dotenv()
app.include_router(bucket_operations_router)
app.include_router(object_operations_router)


@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # await conn.exec_driver_sql("pragma journal_mode=memory")
        # await conn.exec_driver_sql("pragma synchronous=OFF")


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
