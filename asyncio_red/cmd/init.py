import os
from pathlib import Path


def create_python_module(path: Path, name):
    module = path / name
    module.mkdir()
    with (module / '__init__.py').open('w', encoding='utf-8') as f:
        pass
    return module


def init(args):
    current_dir = Path(os.getcwd())
    red_dir = create_python_module(current_dir, name='red')

    with (red_dir / 'config.yaml').open('w', encoding='utf-8') as f:
        f.write(f"app_name: {args.app_name}\n")
        f.write("aws:\n")
        f.write(f"   bucket: {args.s3_bucket}\n")

    registry_dir = create_python_module(red_dir, name='registry')

    with (registry_dir / f'{args.app_name}.py').open('w', encoding='utf-8') as f:
        f.write("from asyncio_red import BaseEvent\n\n\n")
        f.write("# write your schemas extending the `BaseEvent`\n")
