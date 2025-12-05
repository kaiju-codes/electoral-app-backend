from pydantic import BaseModel


class ConstituencyItem(BaseModel):
    """Constituency item in the constituencies list."""
    
    number_english: int | None
    number_local: str | None
    name_english: str | None
    name_local: str | None
    display_name: str


class ConstituenciesResponse(BaseModel):
    """Response for GET /locations/constituencies endpoint."""
    
    constituencies: list[ConstituencyItem]


class StatesResponse(BaseModel):
    """Response for GET /locations/states endpoint."""
    
    states: list[str]

