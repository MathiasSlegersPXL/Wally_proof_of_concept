from fastapi import FastAPI

app = FastAPI()

@app.get("/health")
def health_check():
    return {"Status": "Very Healthy"}
