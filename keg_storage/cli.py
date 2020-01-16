import click
import functools
from flask.cli import with_appcontext
import humanize

from keg_storage.backends.base import FileNotFoundInStorageError
from keg_storage import utils


@click.group('_storage')
@click.option('--location')
@with_appcontext
@click.pass_context
def storage(ctx, location):
    from flask import current_app
    location = location or current_app.config.get('KEG_STORAGE_DEFAULT_LOCATION')
    if not location:
        click.echo('No location given and no default was configured.')
        ctx.abort()
    try:
        ctx.obj.data['storage'] = current_app.storage.get_interface(location)
    except KeyError:
        click.echo('The location {} does not exist. '
                   'Pass --location or change your configuration.'.format(location))
        ctx.abort()


def handle_not_found(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except FileNotFoundInStorageError as e:
            raise click.FileError(
                e.filename,
                hint="Not found in {}.".format(str(e.storage_type))
            )

    return wrapper


@storage.command('list')
@click.option('--simple', is_flag=True)
@click.argument('path', default='/')
@click.pass_context
def storage_list(ctx, path, simple):
    objs = ctx.obj.data['storage'].list(path)

    def fmt(item):
        fmt_str = '{name}' if simple else '{date}\t{size}\t{name}'
        keys = {
            'date': humanize.naturaldate(item.last_modified),
            'name': item.name,
            'size': humanize.naturalsize(item.size, gnu=True),
        }
        return fmt_str.format(**keys)

    lines = [fmt(item) for item in objs]
    click.echo("\n".join(lines))


@storage.command('get')
@click.argument('path')
@click.argument('file', type=click.File(mode='wb', lazy=True), required=False)
@click.pass_context
@handle_not_found
def storage_get(ctx, path, file):
    if file is None:
        file = open(path.split('/')[-1], 'wb')

    ctx.obj.data['storage'].download(path, file)
    click.echo("Downloaded {path} to {dest}.".format(path=path, dest=file.name), err=True)


@storage.command('put')
@click.argument('file', type=click.File(mode='rb'))
@click.argument('key')
@click.pass_context
def storage_put(ctx, file, key):
    ctx.obj.data['storage'].upload(file, key)
    click.echo("Uploaded {path} to {key}.".format(key=key, path=getattr(file, 'name', '-')),
               err=True)


@storage.command('delete')
@click.argument('path')
@click.pass_context
@handle_not_found
def storage_delete(ctx, path):
    ctx.obj.data['storage'].delete(path)
    click.echo("Deleted {path}.".format(path=path), err=True)


@storage.command('link_for')
@click.argument('path')
@click.option('--expiration', '-e', default=3600,
              help="Expiration time (in seconds) of the link, defaults to 1 hours")
@click.pass_context
def storage_link_for(ctx, path, expiration):

    try:
        retval = ctx.obj.data['storage'].link_for(path, expiration)
    except Exception as e:
        click.echo(str(e))
        ctx.abort()

    click.echo("{data}".format(data=retval))


@storage.command('reencrypt')
@click.argument('path')
@click.pass_context
@handle_not_found
def storage_reencrypt(ctx, path):
    old_key = click.prompt('Old Key', hide_input=True).encode('ascii')
    new_key = click.prompt('New Key', hide_input=True).encode('ascii')

    utils.reencrypt(ctx.obj.data['storage'], path, old_key, new_key)
    click.echo('Re-encrypted {path}'.format(path=path))


def add_cli_to_app(app, cli_group_name):
    app.cli.add_command(storage, name=cli_group_name)
