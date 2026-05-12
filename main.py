import asyncio
import time
import uuid
import json
import os
import subprocess
from datetime import datetime, timedelta
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import create_engine, Column, String, Float, DateTime, JSON as SQL_JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# --- 1. DATABASE CONFIGURATION ---
DATABASE_URL = "postgresql://postgres:bmsce@localhost:5432/jenkins_db"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class JobModel(Base):
    __tablename__ = "jobs"
    id = Column(String, primary_key=True)
    repo = Column(String)
    branch = Column(String)
    priority_score = Column(Float)
    status = Column(String) 
    current_stage = Column(String, default="Queued")
    required_skill = Column(String)  # python, nodejs, security, general
    assigned_worker = Column(String, default="Pending")
    files = Column(SQL_JSON) 
    created_at = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(bind=engine)

app = FastAPI()

# --- 2. MULTI-QUEUE SYSTEM (Non-FIFO Priority) ---
# We use separate queues so specialized workers don't block each other
queues = {
    "python": asyncio.PriorityQueue(),
    "nodejs": asyncio.PriorityQueue(),
    "security": asyncio.PriorityQueue(),
    "general": asyncio.PriorityQueue()
}

WEIGHTS = {
    "branch": {"main": 10, "develop": 30, "feature": 50},
    "file_impact_multiplier": 2,
    "conflict_boost": 25
}

# --- 3. DASHBOARD WITH WORKER TRACKING ---
@app.get("/", response_class=HTMLResponse)
async def dashboard():
    db = SessionLocal()
    jobs = db.query(JobModel).order_by(JobModel.created_at.desc()).limit(15).all()
    db.close()
    
    html_content = """
    <html>
        <head>
            <title>Smart Jenkins | Specialized Orchestrator</title>
            <style>
                body { font-family: 'Segoe UI', sans-serif; margin: 0; background: #f4f7f9; }
                header { background: #2c3e50; color: white; padding: 15px; text-align: center; }
                .container { padding: 20px; }
                table { width: 100%; border-collapse: collapse; background: white; border-radius: 10px; overflow: hidden; box-shadow: 0 5px 15px rgba(0,0,0,0.1); }
                th { background: #34495e; color: white; padding: 12px; font-size: 13px; text-transform: uppercase; }
                td { padding: 12px; border-bottom: 1px solid #eee; text-align: left; font-size: 14px; }
                .status-Running { color: #f39c12; font-weight: bold; animation: pulse 1s infinite; }
                .status-Completed { color: #27ae60; font-weight: bold; }
                .worker-tag { background: #e1f5fe; color: #0288d1; padding: 3px 8px; border-radius: 5px; font-size: 11px; font-weight: bold; }
                .skill-tag { background: #f3e5f5; color: #7b1fa2; padding: 3px 8px; border-radius: 5px; font-size: 11px; }
                @keyframes pulse { 50% { opacity: 0.5; } }
            </style>
            <meta http-equiv="refresh" content="2">
        </head>
        <body>
            <header><h1>🚀 Priority Specialized Orchestrator</h1></header>
            <div class="container">
                <table>
                    <tr>
                        <th>Job ID</th><th>Repo</th><th>Branch</th><th>Skill</th><th>Worker</th><th>Prio</th><th>Status</th><th>Stage</th>
                    </tr>
    """
    for job in jobs:
        html_content += f"""
                    <tr>
                        <td><code>{job.id}</code></td>
                        <td><b>{job.repo}</b></td>
                        <td>{job.branch}</td>
                        <td><span class="skill-tag">{job.required_skill}</span></td>
                        <td><span class="worker-tag">{job.assigned_worker}</span></td>
                        <td>{job.priority_score}</td>
                        <td class="status-{job.status}">{job.status}</td>
                        <td>{job.current_stage}</td>
                    </tr>
        """
    return html_content + "</table></div></body></html>"

# --- 4. SCORING & SKILL ASSIGNMENT ---
def get_priority_and_skill(repo, branch, files, db):
    # Skill Logic
    skill = "general"
    if "api" in repo: skill = "python"
    elif "web" in repo: skill = "nodejs"
    elif any("security" in f.lower() for f in files): skill = "security"

    # Priority Logic
    b_score = WEIGHTS["branch"].get(branch, 50)
    for key in WEIGHTS["branch"]:
        if key in branch: b_score = WEIGHTS["branch"][key]; break
    
    f_score = len(files) * WEIGHTS["file_impact_multiplier"]
    conflict_reduction = 0
    ten_mins_ago = datetime.utcnow() - timedelta(minutes=10)
    recent_jobs = db.query(JobModel).filter(JobModel.created_at >= ten_mins_ago).all()
    for j in recent_jobs:
        if any(f in j.files for f in files):
            conflict_reduction = WEIGHTS["conflict_boost"]
            break
            
    return float(b_score + f_score - conflict_reduction), skill

@app.post("/webhook")
async def github_webhook(request: Request):
    payload = await request.json()
    db = SessionLocal()
    
    repo = payload.get("repository", {}).get("name", "unknown")
    branch = payload.get("ref", "").split("/")[-1]
    files = payload.get("head_commit", {}).get("modified", [])
    
    score, skill = get_priority_and_skill(repo, branch, files, db)
    job_id = f"JOB-{uuid.uuid4().hex[:4].upper()}"
    
    new_job = JobModel(id=job_id, repo=repo, branch=branch, priority_score=score, 
                       files=files, status="Queued", required_skill=skill)
    db.add(new_job)
    db.commit()
    db.close()

    # Add to the specific queue
    await queues[skill].put((score, time.time(), job_id))
    return {"status": "queued", "job_id": job_id, "skill": skill}

# --- 5. THE SPECIALIZED WORKER ENGINE ---
async def specialized_worker(worker_name, skill, delay):
    print(f"Worker {worker_name} online for {skill} tasks.")
    while True:
        # Dequeue based on PRIORITY, not entry time
        score, ts, job_id = await queues[skill].get()
        
        db = SessionLocal()
        job = db.query(JobModel).filter(JobModel.id == job_id).first()
        
        if job:
            try:
                job.status = "Running"
                job.assigned_worker = worker_name
                db.commit()

                # REAL GIT ACTION
                repo_path = os.path.abspath(os.path.join(os.getcwd(), job.repo))
                subprocess.run(["git", "-C", repo_path, "checkout", "-f", job.branch], check=True)
                subprocess.run(["git", "-C", repo_path, "pull", "origin", job.branch], check=True)

                # DYNAMIC PIPELINE LOADING
                json_path = os.path.join(repo_path, "pipeline.json")
                with open(json_path, "r") as f:
                    stages = json.load(f)["pipeline"]["stages"]

                for stage in stages:
                    job.current_stage = stage
                    db.commit()
                    await asyncio.sleep(delay)
                
                job.status = "Completed"
                job.current_stage = "Success ✅"
            except Exception as e:
                job.status = "Failed"
                job.current_stage = f"Error: {str(e)[:15]}"
            
            db.commit()
        
        db.close()
        queues[skill].task_done()

# --- 6. STARTUP: 6 WORKERS ---
@app.on_event("startup")
async def startup():
    # 2 Workers for Python (api_core)
    asyncio.create_task(specialized_worker("Python-Alpha", "python", 2.0))
    asyncio.create_task(specialized_worker("Python-Beta", "python", 3.5))
    
    # 1 Worker for NodeJS (web_ui)
    asyncio.create_task(specialized_worker("Node-JS-Runner", "nodejs", 2.5))
    
    # 1 Worker for Security (triggered by 'security' in filename)
    asyncio.create_task(specialized_worker("Security-Scanner", "security", 4.0))
    
    # 2 Workers for everything else
    asyncio.create_task(specialized_worker("General-1", "general", 2.0))
    asyncio.create_task(specialized_worker("General-2", "general", 2.0))