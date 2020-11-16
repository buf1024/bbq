import json

import click

from bbq.common import setup_db, setup_log
from bbq.data.funddb import FundDB
from bbq.data.stockdb import StockDB
from bbq.selector.strategy import strategies


@click.group()
@click.pass_context
@click.option('--uri', type=str, default='mongodb://localhost:27017/',
              help='mongodb connection uri, default: mongodb://localhost:27017/')
@click.option('--pool', default=10, type=int, help='mongodb connection pool size, default: 10')
@click.option('--type', default='stock', type=str, help='selector type, default: stock')
@click.option('--debug/--no-debug', default=True, type=bool, help='show debug log, default: --debug')
def cli(ctx, uri: str, pool: int, typ: str, debug: bool):
    ctx.ensure_object(dict)
    logger = setup_log(debug, 'select.log')
    cls = StockDB
    if typ != 'stock':
        cls = FundDB
    db = setup_db(uri, pool, cls)
    if db is None:
        return

    ctx.obj['db'] = db
    ctx.obj['logger'] = logger


@cli.command()
@click.pass_context
@click.option('--strategy', type=str, help='strategy full name')
def help(ctx, strategy: str):
    if strategy is None or len(strategy) == '':
        print('please provide strategy name, via --strategy argument')
        return

    names = strategies.keys()
    if strategy not in names:
        print('strategy "{}", not found, available names: \n  {}'.format(strategy, '  \n'.join(names)))
        return

    cls_inst = strategies[strategy]()
    print('strategy {}:\n{}'.format(strategy, cls_inst.desc()))


@cli.command()
@click.pass_context
def list(ctx):
    print('strategies: ')
    for strategy in strategies.keys():
        print('  {}'.format(strategy))


@cli.command()
@click.pass_context
@click.option('--strategy', type=str, help='strategy name')
@click.option('--argument', type=str, help='strategy argument, json string')
@click.option('--count', type=int, defalut=10, help='select limit count, default 10')
@click.option('--regression/--no-regression', type=bool, defalut=False, help='auto regression, default False')
def select(ctx, strategy: str, argument: str, count: int, regression: bool):
    names = strategies.keys()
    if strategy not in names:
        print('strategy "{}", not found, available names: \n  {}'.format(strategy, '  \n'.join(names)))
        return
    js = None
    if argument is not None and len(argument) != 0:
        try:
            js = json.loads(argument)
        except Exception as e:
            print('argument is not legal json string, please check --argument')
            return

    cls_inst = strategies[strategy]()
    if not cls_inst.init(js):
        print('strategy init failed')
        return

    codes = cls_inst.select()
    if codes is not None:
        if len(codes) > count:
            codes = codes[:count]
        print('select codes:\n{}'.format(','.join(codes)))
        if regression:
            result = cls_inst.regression(codes)
            print('select regression result:\n{}'.format(result))
    if codes is None:
        print('no code selected')
    cls_inst.destroy()


if __name__ == '__main__':
    cli()
