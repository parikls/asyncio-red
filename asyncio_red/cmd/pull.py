import importlib
import json
import os
from io import BytesIO, StringIO
from pathlib import Path

import aioboto3
from datamodel_code_generator import generate, InputFileType
from ruamel import yaml

EVENT_CLASSES = {}
EVENTS_MAPPING = {}
required_schemas = []


async def pull():
    red_dir = Path(os.getcwd()) / 'red'
    with (red_dir / 'config.yaml').open() as f:
        config = yaml.safe_load(f)

    app_name = config['app_name']
    s3_bucket = config['aws']['bucket']

    session = aioboto3.Session()
    async with session.resource("s3") as s3:
        bucket = await s3.Bucket(s3_bucket)
        async for s3_object in bucket.objects.all():
            app, *_ = s3_object.key.split('/')
            if app == app_name:
                # skip
                continue

            meta = await s3_object.get()

            contents = await meta['Body'].read()
            schema = contents.decode("utf-8")

            generate(
                schema,
                input_file_type=InputFileType.JsonSchema,
                output=red_dir / 'registry' / f'{app}.py',
                use_schema_description=True,
                base_class='asyncio_red.events.BaseEvent'
            )
