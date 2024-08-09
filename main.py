from fastapi import FastAPI,  HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from pydantic import BaseModel, EmailStr
from card_function import get_values_from_db
from card_function_optimized import get_values_from_db_optimized
from retrive_companyid_function import fetch_user_companies
from user_company import get_company_by_email
from typing import List

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class UsernameRequest(BaseModel):
    username: str

class CompanyResponse(BaseModel):
    company_id: int
    company_name: str

@app.post("/get_values")
async def get_values(request: UsernameRequest):
    return await get_values_from_db(request.username)

@app.post("/get_values_optimized")
async def get_values(request: UsernameRequest):
    return await get_values_from_db_optimized(request.username)

@app.get("/user_companies/{email}")
async def get_user_companies(email: str):
    try:
        result = await fetch_user_companies(email)
        # return {"companies": result}
        return result
    except HTTPException as e:
        raise e
    
@app.get("/company/{email}", response_model=List[CompanyResponse])
async def company_lookup(email: EmailStr):
    try:
        result = await get_company_by_email(email)
        return [CompanyResponse(company_id=row['company_id'], company_name=row['company_name']) for row in result]
    except HTTPException as e:
        raise e

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
