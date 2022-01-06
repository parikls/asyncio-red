import asyncio
from argparse import ArgumentParser

from asyncio_red.cmd.init import init
from asyncio_red.cmd.pull import pull
from asyncio_red.cmd.push import push


def main():
    parser = ArgumentParser()
    subparsers = parser.add_subparsers(dest='cmd')

    subparsers.add_parser('push', help='push help')
    subparsers.add_parser('pull', help='pull help')
    init_parser = subparsers.add_parser('init')

    init_parser.add_argument("--app-name", type=str, required=True)
    init_parser.add_argument("--s3-bucket", type=str, required=True)

    args = parser.parse_args()
    if args.cmd == 'init':
        init(args)
    elif args.cmd == 'push':
        asyncio.run(push())
    elif args.cmd == 'pull':
        asyncio.run(pull())
    else:
        raise RuntimeError("Unknown command")


if __name__ == '__main__':
    main()
