from __future__ import annotations

import os
from datetime import datetime

COMMON_TABLE_DATA = [
    {
        "id": 1,
        "usr_name": "Arun_AB",
        "password": os.getenv("unittstpassword"),
        "email": "AB@gmail.com",
        "created_on": datetime.now(),
    },
    {
        "id": 2,
        "usr_name": "Akash Kumar",
        "password": os.getenv("unittstpassword"),
        "email": "akash@hotmail.com",
        "created_on": datetime.now(),
    },
    {
        "id": 3,
        "usr_name": "Praveen",
        "password": os.getenv("unittstpassword"),
        "email": "praveen@yahoo.com",
        "created_on": datetime.now(),
    },
]
