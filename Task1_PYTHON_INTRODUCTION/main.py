import sys
import click

from pymysql.cursors import DictCursor

from database import DatabaseLayer
from configuration import ConfigurationManager
from conversion import ConversionLayer

config_file_path: str
env: str


def execute_and_convert(file_format: str, outfile: str, cursor: DictCursor):
    """
    Executes a conversion operation based on the specified file format.
        Parameters:
            file_format (str): The desired file format for the conversion. Valid options are 'xml' and 'json'.
            outfile (str): The file path or buffer where the converted data will be saved.
            cursor (DictCursor): The select-query cursor.
    """
    if file_format == 'xml':
        ConversionLayer.convert_to_xml(cursor, outfile)
    elif file_format == 'json':
        ConversionLayer.convert_to_json(cursor, outfile)


@click.group
@click.option('-e', '--environment', 'env_arg', default='dev', help='Environment configuration')
@click.option('-c', '--configuration-path', 'config_file_path_arg', default='./db_configs/mysql_config.ini',
              help='Configuration file path')
@click.option('-o', '--output', 'outfile', type=click.File('wb'), default=sys.stdout.buffer,
              help='Output file path')
@click.option('-f', '--format', 'file_format', type=click.Choice(['xml', 'json']), default='xml',
              help='Format of the result')
@click.pass_context
def cli(ctx: click.Context, env_arg: str, config_file_path_arg: str, file_format: str, outfile: str):
    """
    Command-line interface (CLI) function for database operations.
        Parameters:
            ctx (click.Context): The Click context object.
            env_arg (str): The environment configuration argument.
            config_file_path_arg (str): The configuration file path argument.
            file_format (str): The format of the result. Valid options are 'xml' and 'json'.
            outfile (str): The output file path.

    """
    global env, config_file_path
    env, config_file_path = env_arg, config_file_path_arg

    ctx.ensure_object(dict)
    ctx.obj['file_format'] = file_format
    ctx.obj['outfile'] = outfile


@cli.command()
@click.pass_context
def process_query1(ctx: click.Context):
    """
    Save rooms and number of students in specified format.
        Parameters:
            ctx (click.Context): The Click context object.
    """
    with DatabaseLayer(ConfigurationManager.get_configuration(config_file_path, env)) as db:
        execute_and_convert(ctx.obj['file_format'], ctx.obj['outfile'], db.get_rooms_with_occupancy())


@cli.command()
@click.pass_context
def process_query2(ctx: click.Context):
    """
    Save 5 rooms with the smallest average age in specified format.
        Parameters:
            ctx (click.Context): The Click context object.
    """
    with DatabaseLayer(ConfigurationManager.get_configuration(config_file_path, env)) as db:
        execute_and_convert(ctx.obj['file_format'], ctx.obj['outfile'], db.get_rooms_with_smallest_average_age())


@cli.command()
@click.pass_context
def process_query3(ctx: click.Context):
    """
    Save 5 rooms with the biggest age difference specified format.
        Parameters:
            ctx (click.Context): The Click context object.
    """
    with DatabaseLayer(ConfigurationManager.get_configuration(config_file_path, env)) as db:
        execute_and_convert(ctx.obj['file_format'], ctx.obj['outfile'], db.get_rooms_with_biggest_age_difference())


@cli.command()
@click.pass_context
def process_query4(ctx: click.Context):
    """
    Save rooms with mixed genders in specified format.
        Parameters:
            ctx (click.Context): The Click context object.
    """
    with DatabaseLayer(ConfigurationManager.get_configuration(config_file_path, env)) as db:
        execute_and_convert(ctx.obj['file_format'], ctx.obj['outfile'], db.get_rooms_with_mixed_genders())


if __name__ == '__main__':
    cli()
