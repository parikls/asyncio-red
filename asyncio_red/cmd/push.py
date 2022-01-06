import importlib
import json
import os
from io import BytesIO
from pathlib import Path

import aioboto3
from pydantic.schema import schema
from ruamel import yaml

from asyncio_red.events import BaseEvent


async def push():
    red_dir = Path(os.getcwd()) / 'red'
    with (red_dir / 'config.yaml').open() as f:
        config = yaml.safe_load(f)

    app_name = config['app_name']
    s3_bucket = config['aws']['bucket']

    importlib.import_module(f'red.registry.{app_name}')
    scope = BaseEvent.__subclasses__()

    schema_dump = schema(scope, title=f'{app_name} schema')
    f_obj = BytesIO(json.dumps(schema_dump).encode("utf-8"))
    f_obj.seek(0)

    session = aioboto3.Session()
    async with session.client("s3") as s3:
        await s3.upload_fileobj(f_obj, s3_bucket, f'{app_name}/schema.json')

