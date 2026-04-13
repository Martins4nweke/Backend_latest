from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os
from app.routes.allocate import router as allocation_router
from app.routes.disease_modules import router as disease_router

app = FastAPI(
    title="NCD Cascade Allocation Backend",
    version="6.3.0",
    description="Generic NCD care-cascade allocation backend with evidence retrieval waterfall, diagnostics, policy intelligence, and parameter-level provenance.",
)

frontend_origins = os.getenv("FRONTEND_ORIGINS", "").strip()
allow_origins = [o.strip() for o in frontend_origins.split(",") if o.strip()] if frontend_origins else ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def healthcheck():
    return {"status": "ok", "message": "NCD cascade allocation backend v6.3 is running."}

app.include_router(disease_router, prefix="/api/v1", tags=["disease_modules"])
app.include_router(allocation_router, prefix="/api/v1", tags=["allocation"])
