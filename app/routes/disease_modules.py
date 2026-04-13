from fastapi import APIRouter, HTTPException
from app.services.disease_registry import list_modules, get_module
router = APIRouter()
@router.get('/disease-modules')
def get_disease_modules():
    return {'disease_modules': list_modules()}
@router.get('/disease-modules/{disease}')
def get_single_disease_module(disease: str):
    module = get_module(disease)
    if module is None:
        raise HTTPException(status_code=404, detail='Disease module not found.')
    return module
