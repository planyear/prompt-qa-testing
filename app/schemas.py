from pydantic import BaseModel
from typing import List

class OutputRow(BaseModel):
    row: int
    csv_file_id: str
    csv_file_name: str

class RunJobResult(BaseModel):
    outputs: List[OutputRow]
