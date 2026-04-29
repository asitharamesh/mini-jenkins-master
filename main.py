import asyncio
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
import socketio
import random
import json
from sqlalchemy import create_engine, Column, String, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# --- POSTGRES CONFIGURATION ---
DATABASE_URL = "postgresql://postgres:bmsce@localhost:5432/jenkins_db"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class JobModel(Base):
    __tablename__ = "jobs"
    id = Column(String, primary_key=True, index=True)
    repo = Column(String)
    commit = Column(String)
    commit_msg = Column(String)  # NEW COLUMN FOR COMMIT MESSAGE
    lang = Column(String)
    status = Column(String)
    worker = Column(String)
    stages_json = Column(Text)

# Create/Update tables in the DB
Base.metadata.create_all(bind=engine)

# --- APP SETUP ---
sio = socketio.AsyncServer(async_mode='asgi', cors_allowed_origins='*')
app = FastAPI()
socket_app = socketio.ASGIApp(sio, app)

WORKERS = {
    "Worker-Alpha": {"lang": "Python", "busy": False},
    "Worker-Beta": {"lang": "NodeJS", "busy": False},
    "Worker-Gamma": {"lang": "C++", "busy": False},
    "Worker-Delta": {"lang": "Python", "busy": False}
}

pending_queue = asyncio.Queue()

def detect_language(payload):
    head_commit = payload.get("head_commit") or {}
    files = head_commit.get("added", []) + head_commit.get("modified", [])
    for file in files:
        if file.endswith(".py"): return "Python"
        if file.endswith(".js") or file == "package.json": return "NodeJS"
        if file.endswith(".cpp") or file.endswith(".h"): return "C++"
    return "Python"

async def update_job_in_db(job_data):
    db = SessionLocal()
    try:
        job = db.query(JobModel).filter(JobModel.id == job_data["id"]).first()
        if job:
            job.status = job_data["status"]
            job.worker = job_data["worker"]
            job.stages_json = json.dumps(job_data["stages"])
            db.commit()
    finally:
        db.close()
    await sio.emit('job_updated', job_data)

async def run_pipeline(job_id, worker_name):
    WORKERS[worker_name]["busy"] = True
    db = SessionLocal()
    job_rec = db.query(JobModel).filter(JobModel.id == job_id).first()
    job_data = {
        "id": job_rec.id, "repo": job_rec.repo, "commit": job_rec.commit,
        "commit_msg": job_rec.commit_msg, # PASSING TO UI
        "lang": job_rec.lang, "status": "In Progress", "worker": worker_name,
        "stages": json.loads(job_rec.stages_json)
    }
    db.close()
    await update_job_in_db(job_data)
    
    for stage in ["Fetch Code", "Security Scan", "Build", "Push Image"]:
        job_data["stages"][stage] = "in-progress"
        await update_job_in_db(job_data)
        await asyncio.sleep(random.uniform(2, 4))
        job_data["stages"][stage] = "completed"
        await update_job_in_db(job_data)
    
    job_data["status"] = "Completed"
    WORKERS[worker_name]["busy"] = False 
    await update_job_in_db(job_data)

async def scheduler():
    while True:
        if not pending_queue.empty():
            job_id = await pending_queue.get()
            db = SessionLocal()
            job_rec = db.query(JobModel).filter(JobModel.id == job_id).first()
            if job_rec:
                worker = next((n for n, i in WORKERS.items() if i["lang"] == job_rec.lang and not i["busy"]), None)
                if worker:
                    asyncio.create_task(run_pipeline(job_id, worker))
                else:
                    await pending_queue.put(job_id)
            db.close()
        await asyncio.sleep(0.5)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(scheduler())

@app.post("/webhook")
async def github_webhook(request: Request):
    payload = await request.json()
    repo_name = payload.get("repository", {}).get("name", "Unknown-Repo")
    head_commit = payload.get("head_commit") or {}
    
    # NEW: Extract the commit message
    commit_msg = head_commit.get("message", "No message provided")
    commit_id = payload.get("after", "000000")[:7]
    
    print(f"Incoming Build: {commit_msg}") # Helpful for your terminal
    
    lang = detect_language(payload)
    job_id = f"J-{random.randint(1000, 9999)}"
    stages = {s: "pending" for s in ["Fetch Code", "Security Scan", "Build", "Push Image"]}
    
    db = SessionLocal()
    db.add(JobModel(
        id=job_id, 
        repo=repo_name, 
        commit=commit_id, 
        commit_msg=commit_msg, # SAVING TO DB
        lang=lang, 
        status="Queued", 
        worker="Waiting...", 
        stages_json=json.dumps(stages)
    ))
    db.commit()
    db.close()

    await sio.emit('job_updated', {
        "id": job_id, "repo": repo_name, "commit": commit_id, 
        "commit_msg": commit_msg, "lang": lang, "status": "Queued", 
        "worker": "Waiting...", "stages": stages
    })
    await pending_queue.put(job_id)
    return {"status": "queued"}

app.mount("/", StaticFiles(directory="public", html=True), name="public")