''' Script used to provide API access point to the converter to make the system scalable via Docker.
The script also provides additional checks and logging functionalities to monitoring.'''

from fastapi import FastAPI

app = FastAPI()

@app.get("/status")
async def root():
    return 200

@app.post("/ingest_file/{s3_url}")
def root(s3_url: str):
    return 200

@app.get("/get_filtered_data")
def root():
    pass