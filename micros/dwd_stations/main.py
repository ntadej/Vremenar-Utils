"""Deta Micro for DWD stations information."""
from deta import Deta  # type: ignore
from fastapi import FastAPI
from typing import List

deta = Deta()
db = deta.AsyncBase('dwd_stations')
app = FastAPI()  # notice that the app instance is called `app`, this is very important.


@app.get('/mosmix')
async def mosmix() -> List[str]:
    """List MOSMIX stations keys."""
    last_item = None
    total_count = 0
    keys: List[str] = []
    while True:
        result = await db.fetch(last=last_item)
        total_count += result.count
        for item in result.items:
            keys.append(item['key'])
        last_item = result.last
        if not last_item:
            break
    return keys
