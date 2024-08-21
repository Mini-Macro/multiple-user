from fastapi import FastAPI,  HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from pydantic import BaseModel, EmailStr
from card_function import get_values_from_db
from card_function_optimized import get_values_from_db_optimized
from company_detail_using_companyid import get_values_using_company_id
from user_company import get_company_by_email
from ipd_opd_revenue import main
from best_worst_performing_dep import get_total_voucher_amount
# from ipd_opd_revenue_optimize import main
from typing import List

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class CompanyIDRequest(BaseModel):
    company_id: int

class CompanyResponse(BaseModel):
    company_id: int
    company_name: str

class DateRange(BaseModel):
    start_date: str
    end_date: str
    company_id: int

@app.post("/get_values")
async def get_values(request: CompanyIDRequest):
    return await get_values_from_db(request.username)

@app.post("/get_values_optimized")
async def get_values(request: CompanyIDRequest):
    return await get_values_from_db_optimized(request.company_id)

@app.post("/get_values_using_companyid")
async def get_values(request: CompanyIDRequest):
    return await get_values_using_company_id(request.company_id)

@app.get("/company/{email}", response_model=List[CompanyResponse])
async def company_lookup(email: EmailStr):
    try:
        result = await get_company_by_email(email)
        return [CompanyResponse(company_id=row['company_id'], company_name=row['company_name']) for row in result]
    except HTTPException as e:
        raise e

@app.post("/ipd_opd_revenue/")
async def monthly_totals(date_range: DateRange):
    try:
        result = await main(date_range.start_date, date_range.end_date, date_range.company_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/total_voucher_amount/")
async def total_voucher_amount(request: CompanyIDRequest):
    try:
        result = await get_total_voucher_amount(request.company_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
