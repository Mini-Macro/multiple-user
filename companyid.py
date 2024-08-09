# app/main.py
from fastapi import FastAPI, HTTPException
from retrive_companyid_function import fetch_user_companies
import uvicorn

app = FastAPI()

@app.get("/user_companies/{email}")
async def get_user_companies(email: str):
    try:
        company_ids = await fetch_user_companies(email)
        return {"companies": company_ids}
    except HTTPException as e:
        raise e

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
