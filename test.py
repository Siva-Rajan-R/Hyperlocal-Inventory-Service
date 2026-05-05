from fastapi import FastAPI,HTTPException
from pydantic import BaseModel


DATABASE={
    "1":{
        'name':'siva','dob':'theriyathu','parent_name':'--','mobile_no':''
    }
}

app=FastAPI()

class FilteredResponse(BaseModel):
    name:str
    dob:str


@app.get('/search/{id}',response_model=FilteredResponse)
def search_by_id(id:str):
    return DATABASE.get(id,"Kelambu")


