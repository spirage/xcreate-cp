from fastapi import FastAPI
from fastapi.openapi.docs import get_swagger_ui_html
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse

from server.api.routers.common_router import *
from server.api.routers.mat_detail_router import *
from server.api.routers.rd_project_router import *
from server.api.routers.ba_rd_split_router import *
from server.api.routers.ba_mat_rd_router import *
from server.api.routers.ba_mat_prod_router import *
from server.api.routers.ba_result_router import *


async def not_found(request, exc):
    return JSONResponse(content=fail(404, "路径不存在"), status_code=exc.status_code)


exception_handlers = {404: not_found}

app = FastAPI(title="XCP 接口文档", description="核心处理单元(Core Processor)接口文档", version="1.9.7", docs_url=None, redoc_url=None, exception_handlers=exception_handlers)


@app.get("/doc", include_in_schema=False)
async def swagger_ui_html():
    return get_swagger_ui_html(
        openapi_url="/openapi.json",
        title="接口文档",
        swagger_favicon_url=None,
        swagger_ui_parameters=None
    )


@app.middleware("http")
async def custom_header(request, call_next):
    response = await call_next(request)
    response.headers["Server"] = "Xcp Server"
    return response

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许所有域，也可以指定特定域
    allow_credentials=True,
    allow_methods=["*"],  # 允许所有方法
    allow_headers=["*"],  # 允许所有头
)

app.include_router(common_router, prefix="/api/common")
app.include_router(mat_detail_router, prefix="/api/mat_detail")
app.include_router(rd_project_router, prefix="/api/rd_project")
app.include_router(ba_rd_split_router, prefix="/api/ba_process")
app.include_router(ba_mat_rd_router, prefix="/api/ba_process")
app.include_router(ba_mat_prod_router, prefix="/api/ba_process")
app.include_router(ba_result_router, prefix="/api/ba_result")
